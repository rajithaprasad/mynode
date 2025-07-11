// server.js
require('dotenv').config(); // Load environment variables from .env file
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Hardcode your Gemini API Key from environment variables
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

// HARDCODED RECOMMENDED MODEL FOR LIVE API
// This model is specifically designed for Live API audio interactions.
const HARDCODED_GEMINI_MODEL = "gemini-2.0-flash-live-001";

if (!GEMINI_API_KEY) {
    console.error('Error: GEMINI_API_KEY not found in .env file. Please create a .env file with GEMINI_API_KEY=YOUR_KEY_HERE');
    process.exit(1);
}

// Serve static files (your index.html and any other frontend assets)
app.use(express.static(path.join(__dirname, 'public'))); // Assuming index.html is in a 'public' folder

wss.on('connection', ws => {
    console.log('Client connected to WebSocket server');

    let geminiWs = null; // WebSocket connection to Gemini API

    // Handle messages from the client
    ws.on('message', async message => {
        try {
            const clientMessage = JSON.parse(message); // Client always sends JSON

            if (clientMessage.type === 'connect') {
                // We're no longer extracting the model from the client message
                const { systemPrompt } = clientMessage; // Still get systemPrompt from client

                const geminiWsUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;
                
                geminiWs = new WebSocket(geminiWsUrl);

                geminiWs.onopen = () => {
                    console.log('Connected to Gemini Live API');
                    ws.send(JSON.stringify({ type: 'status', message: 'Connected to Gemini Live API', alertType: 'success' }));
                    
                    const setupMessage = {
                        setup: {
                            model: `models/${HARDCODED_GEMINI_MODEL}`, // <--- USING HARDCODED RECOMMENDED MODEL HERE
                            generationConfig: {
                                temperature: 0.7,
                                maxOutputTokens: 2048,
                                responseModalities: ["AUDIO"],
                            },
                            realtimeInputConfig: {
                                activityHandling: "START_OF_ACTIVITY_INTERRUPTS"
                            },
                            inputAudioTranscription: {},
                            outputAudioTranscription: {}
                        }
                    };

                    if (systemPrompt) {
                        setupMessage.setup.systemInstruction = { parts: [{ text: systemPrompt }] };
                    }
                    geminiWs.send(JSON.stringify(setupMessage));
                    ws.send(JSON.stringify({ type: 'log', message: 'Setup message sent to Gemini API.', logType: 'info' }));
                };

                geminiWs.onmessage = async geminiMessage => {
                    // Always assume messages from Gemini are text (JSON) containing base64 audio
                    let data;
                    if (geminiMessage.data instanceof Buffer) {
                         // If it's a buffer, convert to string (assuming it's a JSON string)
                        data = JSON.parse(geminiMessage.data.toString());
                    } else {
                        // Otherwise, it's already a string
                        data = JSON.parse(geminiMessage.data);
                    }
                    
                    if (data.serverContent) {
                        const content = data.serverContent;
                        
                        // Handle interruption
                        if (content.interrupted) {
                            ws.send(JSON.stringify({ type: 'interrupted' }));
                            ws.send(JSON.stringify({ type: 'log', message: '🔇 Interrupted by user - stopping audio playback', logType: 'info' }));
                        }
                        
                        // Handle incoming audio from the model
                        if (content.modelTurn && content.modelTurn.parts && !content.interrupted) {
                            for (const part of content.modelTurn.parts) {
                                if (part.inlineData && part.inlineData.data && part.inlineData.mimeType && part.inlineData.mimeType.includes('audio')) {
                                    // Send audio data as base64 string within a JSON object
                                    ws.send(JSON.stringify({
                                        type: 'audio',
                                        data: part.inlineData.data, // This is already base64 PCM from Gemini
                                        mimeType: part.inlineData.mimeType
                                    }));
                                }
                            }
                        }
                        
                        // Handle transcriptions for logging
                        if(content.inputTranscription && content.inputTranscription.text) {
                            ws.send(JSON.stringify({ type: 'log', message: `Heard: "${content.inputTranscription.text}"`, logType: 'user' }));
                        }
                        if(content.outputTranscription && content.outputTranscription.text) {
                            ws.send(JSON.stringify({ type: 'log', message: `Saying: "${content.outputTranscription.text}"`, logType: 'model' }));
                        }
                        if (content.turnComplete) {
                            ws.send(JSON.stringify({ type: 'log', message: 'Turn completed', logType: 'info' }));
                        }
                    } else if (data.setupComplete) {
                         ws.send(JSON.stringify({ type: 'log', message: 'Gemini setup completed successfully', logType: 'success' }));
                    } else if (data.usageMetadata) {
                        ws.send(JSON.stringify({ type: 'log', message: `Usage - Prompt tokens: ${data.usageMetadata.promptTokenCount}, Response tokens: ${data.usageMetadata.responseTokenCount}`, logType: 'info' }));
                    } else {
                        ws.send(JSON.stringify({ type: 'log', message: `Gemini Server: ${JSON.stringify(data).substring(0, 200)}...`, logType: 'info' }));
                    }
                };
                
                geminiWs.onclose = (event) => {
                    console.log(`Gemini WebSocket closed: ${event.code} - ${event.reason || 'No reason given'}`);
                    ws.send(JSON.stringify({ type: 'status', message: 'Disconnected from Gemini Live API', alertType: 'danger' }));
                    ws.send(JSON.stringify({ type: 'log', message: `Gemini WebSocket closed: ${event.code} - ${event.reason || 'No reason given'}`, logType: 'error' }));
                };

                geminiWs.onerror = (error) => {
                    console.error('Gemini WebSocket error:', error);
                    ws.send(JSON.stringify({ type: 'status', message: 'Gemini API Connection Error', alertType: 'danger' }));
                    ws.send(JSON.stringify({ type: 'log', message: `Gemini WebSocket error: ${error.message || 'Unknown error'}`, logType: 'error' }));
                };

            } else if (clientMessage.type === 'audioChunk' && geminiWs && geminiWs.readyState === WebSocket.OPEN) {
                // Forward audio chunks received from client directly to Gemini API
                // clientMessage.data is already base64
                const geminiAudioMessage = {
                    realtimeInput: {
                        mediaChunks: [{
                            mimeType: "audio/pcm;rate=16000",
                            data: clientMessage.data // This is already base64
                        }]
                    }
                };
                geminiWs.send(JSON.stringify(geminiAudioMessage));

            } else if (clientMessage.type === 'endOfStream' && geminiWs && geminiWs.readyState === WebSocket.OPEN) {
                // Forward end of stream message to Gemini API
                const endMessage = {
                    realtimeInput: {
                        mediaChunks: []
                    }
                };
                geminiWs.send(JSON.stringify(endMessage));
                ws.send(JSON.stringify({ type: 'log', message: 'End of audio stream sent to Gemini.', logType: 'info' }));
            }
        } catch (error) {
            console.error('Error processing client message:', error);
            ws.send(JSON.stringify({ type: 'log', message: `Server error: ${error.message}`, logType: 'error' }));
        }
    });

    ws.on('close', () => {
        console.log('Client disconnected from WebSocket server');
        if (geminiWs) {
            geminiWs.close(); // Close Gemini connection when client disconnects
        }
    });

    ws.onerror = (error) => {
        console.error('Client WebSocket error:', error);
    };
});

const PORT = process.env.PORT || 3014;
server.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
    console.log(`WebSocket server listening on ws://localhost:${PORT}`);
});