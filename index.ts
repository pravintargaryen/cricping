import express, { Request, Response } from 'express';
import Fluvio from '@fluvio/client';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';

const app = express();
const PORT = 3000;
const TOPIC_NAME = "cricket-commentary";
const PARTITION = 0;

// Create Fluvio Client Instance
const fluvio = new Fluvio();

// Initialize SQLite database
let db: Database;
const lastCommentIds = new Map<WebSocket, number>();

async function initializeDatabase() {
  db = await open({
    filename: './cricket.db',
    driver: sqlite3.Database
  });

  // Create tables if they do not exist
  await db.exec(`
    CREATE TABLE IF NOT EXISTS comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      content TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS score (
      id INTEGER PRIMARY KEY CHECK (id = 1), 
      runs INTEGER NOT NULL DEFAULT 0,
      wickets INTEGER NOT NULL DEFAULT 0
    );

    INSERT OR IGNORE INTO score (id, runs, wickets) VALUES (1, 0, 0);
  `);
}

// Initialize Fluvio and Database
async function initializeFluvio() {
  try {
    console.log("Connecting client to Fluvio");
    await fluvio.connect();
    console.log("Fluvio client connected");
    await createTopic(); // Create the topic when Fluvio is connected
  } catch (error) {
    console.error("Failed to connect to Fluvio:", error);
  }
}

// Function to create a topic in Fluvio
async function createTopic() {
  try {
    console.log("Creating topic");
    const admin = await fluvio.admin();
    await admin.createTopic(TOPIC_NAME);
    console.log(`Topic '${TOPIC_NAME}' created successfully`);
  } catch (ex) {
    console.log("Topic already exists or error creating topic:", ex);
  }
}

// Serve the static HTML file
app.get('/', (req: Request, res: Response) => {
  res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Cric Ping</title>
	  <!-- Bootstrap CDN -->
  <link href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" rel="stylesheet">
  <!-- Google Fonts -->
  <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;700&display=swap" rel="stylesheet">
  <style>
    body {
      font-family: 'Roboto', sans-serif;
    }
    .container {
      margin-top: 20px;
    }
    #commentsDiv {
      margin-top: 20px;
      border: 1px solid #ddd;
      padding: 10px;
      height: 300px;
      overflow-y: auto;
    }
    textarea {
      width: 100%;
    }
  </style>
    </head>
    <body>
       <div class="container">
    <h1 class="text-center">Cric Ping</h1>
    <div class="form-group">
      <textarea id="commentaryInput" class="form-control" rows="4" placeholder="Enter commentary here..."></textarea>
    </div>
    <div class="form-group">
      <input id="scoreInput" class="form-control" type="number" placeholder="Enter runs scored..." min="0" max="6">
    </div>
    <div class="form-group">
      <input id="wicketInput" class="form-control" type="number" placeholder="Enter wickets lost..." min="0" max="10">
    </div>
    <button class="btn btn-primary" onclick="sendComment()">Send Comment</button>
    <div id="commentsDiv"></div>
  </div>

      <script>
        const ws = new WebSocket('ws://localhost:${PORT}');
        
        // Send comment to server
        function sendComment() {
          const commentaryInput = document.getElementById('commentaryInput').value;
          const scoreInput = document.getElementById('scoreInput').value;
          const wicketInput = document.getElementById('wicketInput').value;

          if (commentaryInput.trim()) {
            const message = {
              commentary: commentaryInput,
              runs: scoreInput || 0,
              wickets: wicketInput || 0
            };
            ws.send(JSON.stringify(message));
            document.getElementById('commentaryInput').value = ''; // Clear input
            document.getElementById('scoreInput').value = ''; // Clear input
            document.getElementById('wicketInput').value = ''; // Clear input
          }
        }

        // Receive comments and display them
        ws.onmessage = function(event) {
          const commentsDiv = document.getElementById('commentsDiv');
          const newComment = document.createElement('div');
          newComment.innerHTML = event.data;
          commentsDiv.insertBefore(newComment, commentsDiv.firstChild); // Add new comments at the top
        };

        // Request initial comments
        ws.onopen = function() {
          ws.send('GET_INITIAL_COMMENTS');
        };
      </script>
    </body>
    </html>
  `);
});

// Create HTTP server and bind it to Express app
const server = http.createServer(app);

// WebSocket server for real-time comments
const wss = new WebSocketServer({ server });

wss.on('connection', (ws: WebSocket) => {
  // Initialize lastCommentId for new connection
  lastCommentIds.set(ws, 0);

  ws.on('message', async (message: string) => {
    try {
      if (message === 'GET_INITIAL_COMMENTS') {
        // Send initial comments
        const lastId = lastCommentIds.get(ws) || 0;
        const rows: { id: number; content: string }[] = await db.all('SELECT id, content FROM comments WHERE id > ? ORDER BY id ASC', [lastId]);
        const score = await db.get('SELECT * FROM score WHERE id = 1');
        rows.forEach(row => {
          ws.send(`Score: ${score.runs}/${score.wickets}<br>${row.content}`);
          lastCommentIds.set(ws, row.id); // Update last comment ID
        });
      } else {
        const parsedMessage = JSON.parse(message);
        const { commentary, runs, wickets } = parsedMessage;

        // Update the score in the database
        await db.run('UPDATE score SET runs = runs + ?, wickets = wickets + ? WHERE id = 1', [runs, wickets]);

        // Ensure Fluvio is connected
        await initializeFluvio();

        // Produce message to Fluvio
        const producer = await fluvio.topicProducer(TOPIC_NAME);
        await producer.send("commentary-key", commentary);

        // Insert message into SQLite database
        const result = await db.run('INSERT INTO comments (content) VALUES (?)', [commentary]);
        const newCommentId = result.lastID as number | undefined;

        const score = await db.get('SELECT * FROM score WHERE id = 1');
        const combinedMessage = `Score: ${score.runs}/${score.wickets}<br>${commentary}`;

        if (newCommentId !== undefined) {
          // Send new comments to all clients
          wss.clients.forEach(async (client: WebSocket) => {
            if (client.readyState === WebSocket.OPEN) {
              const lastId = lastCommentIds.get(client) || 0;
              if (newCommentId > lastId) {
                const rows: { id: number; content: string }[] = await db.all('SELECT id, content FROM comments WHERE id > ? ORDER BY id ASC', [lastId]);
                rows.forEach(row => {
                  client.send(combinedMessage);
                  lastCommentIds.set(client, row.id); // Update last comment ID
                });
              }
            }
          });
        } else {
          console.error('Failed to insert comment into database');
        }
      }
    } catch (error) {
      console.error("Error processing message:", error);
    }
  });

  // When a client connects, send the last comments from SQLite
  (async () => {
    try {
      const lastId = lastCommentIds.get(ws) || 0;
      const rows: { id: number; content: string }[] = await db.all('SELECT id, content FROM comments WHERE id > ? ORDER BY id ASC', [lastId]);
      const score = await db.get('SELECT * FROM score WHERE id = 1');
      rows.forEach(row => {
        ws.send(`Score: ${score.runs}/${score.wickets}<br>${row.content}`);
        lastCommentIds.set(ws, row.id); // Update last comment ID
      });
    } catch (error) {
      console.error("Error retrieving comments:", error);
    }
  })();
});

// Start the server and initialize database and Fluvio connection
server.listen(PORT, async () => {
  console.log(`Server running on http://localhost:${PORT}`);
  await initializeDatabase();
  await initializeFluvio(); // Initialize Fluvio connection and create the topic at server startup
});

