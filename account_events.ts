/**
 * Live Account Event Streamer for Vercel
 * 
 * Features:
 * - Streams live events related to a specific account using gRPC
 * - Emits events via Server-Sent Events (SSE) in real-time
 * - Robust gRPC connection handling with automatic reconnection
 * - Security features for production deployment
 * 
 * Requirements:
 * - An Aptos API key (get one from https://aptoslabs.com/developers)
 * - Set the API key in environment variable APTOS_API_KEY_TESTNET
 * - Vercel deployment
 */

import { streamTransactions } from ".";
import type { VercelRequest, VercelResponse } from '@vercel/node';

// Configuration
const STARTING_VERSION = Number(process.env.STARTING_VERSION || "0");
const MODULE_ADDRESS = process.env.MODULE_ADDRESS || "0xf57ffdaa57e13bc27ac9b46663749a5d03a846ada4007dfdf1483d482b48dace";
const MAX_RETRIES = 5;
const RETRY_DELAY = 5000; // 5 seconds

// Helper function to check if an event is related to our target account
function isAccountRelatedEvent(event: any): boolean {
  try {
    const eventData = JSON.parse(event.data);

    // MarketCreated event
    if (event.typeStr === `${MODULE_ADDRESS}::truthoracle::MarketCreated`) {
      return true;
    }

    // Buy shares event
    if (event.typeStr === `${MODULE_ADDRESS}::truthoracle::buy_shares`) {
      return true;
    }

    // Withdraw payout event
    if (event.typeStr === `${MODULE_ADDRESS}::truthoracle::withdraw_payout`) {
      return true;
    }
    
    return false;
  } catch (error) {
    console.error("Error checking event:", error);
    return false;
  }
}

// Main processing loop with retry logic
async function streamLiveEvents(res: VercelResponse, retryCount = 0, currentVersion = STARTING_VERSION) {
  try {
    console.log(`Starting stream from version ${currentVersion} (attempt ${retryCount + 1}/${MAX_RETRIES})`);
    
    // Set SSE headers
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });

    // Send initial connection message
    res.write(`data: ${JSON.stringify({ type: "connection_status", status: "connected", timestamp: Date.now() })}\n\n`);
    
    for await (const event of streamTransactions({
      url: "grpc.testnet.aptoslabs.com:443",
      apiKey: process.env.APTOS_API_KEY_TESTNET!,
      startingVersion: BigInt(currentVersion),
    })) {
      switch (event.type) {
        case "data": {
          if (event.chainId !== 2n) {
            throw new Error(
              `Transaction stream returned a chainId of ${event.chainId}, but expected testnet chainId=2`
            );
          }

          console.log('transactionLength', event.transactions.length, 'currentVersion', currentVersion);

          // Process each transaction
          for (const txn of event.transactions) {
            const version = txn.version!;
            const timestamp = Number(txn.timestamp.seconds)!;

            // Process each event in the transaction
            for (const evt of txn?.user?.events || []) {
              if (isAccountRelatedEvent(evt)) {
                console.log({ evt });

                // Send event to client
                res.write(`data: ${JSON.stringify({
                  type: "account_event",
                  data: {
                    version,
                    event_type: evt.typeStr,
                    event_data: JSON.parse(evt.data),
                    timestamp,
                  },
                })}\n\n`);
              }
            }
          }
          break;
        }
        case "error": {
          console.error("Stream error:", event.error);
          // Check for connection drop
          if (event.error.code === 14 && event.error.details === "Connection dropped") {
            console.log(`Connection dropped, restarting from version ${currentVersion}`);
            return await streamLiveEvents(res, 0, currentVersion);
          }
        }
        case "metadata": {
          break;
        }
        case "status": {
          if (event.status.code !== 0) {
            console.error(`Stream status error: ${event.status.code} - ${event.status.details}`);
            // Check for wire type error
            if (event.status.code === 13 && event.status.details.includes("invalid wire type")) {
              console.log(`Encountered wire type error, incrementing version from ${currentVersion} to ${currentVersion + 1}`);
              return await streamLiveEvents(res, 0, currentVersion + 1);
            }
            // Check for connection drop
            if (event.status.code === 14 && event.status.details === "Connection dropped") {
              console.log(`Connection dropped, restarting from version ${currentVersion}`);
              return await streamLiveEvents(res, 0, currentVersion);
            }
          }
          break;
        }
      }
    }
  } catch (error) {
    console.error("Error in main processing loop:", error);
    
    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying in ${RETRY_DELAY/1000} seconds... (${retryCount + 1}/${MAX_RETRIES})`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
      return await streamLiveEvents(res, retryCount + 1, currentVersion);
    } else {
      console.error("Max retries reached. Please check your connection and API key.");
      res.end();
    }
  }
}

// Vercel serverless function handler
export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method === 'GET') {
    await streamLiveEvents(res);
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
} 