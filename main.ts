// main.ts
import "dotenv/config";
import { Client } from "pg";
import { DateTime } from "luxon";

// Vehicle data type from API
interface Vehicle {
  id: string;
  label: string;
  vehicle_type: string;
  latitude: number;
  longitude: number;
  timestamp: string;
  speed: number;
  bike_accessible: string;
  wheelchair_accessible: string;
}

// Environment variable validation
const requiredEnvVars = [
  "DB_HOST",
  "DB_PORT",
  "DB_USER",
  "DB_PASSWORD",
  "DB_NAME",
  "TRANZY_API_URL",
  "TRANZY_API_KEY",
];

function validateEnvironment() {
  const missing = requiredEnvVars.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    console.error(`❌ Missing required environment variables: ${missing.join(", ")}`);
    process.exit(1);
  }
  console.log("✅ All required environment variables are set");
}

async function createDbConnection() {
  try {
    const client = new Client({
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT),
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
    });

    await client.connect();
    console.log("✅ Connected to PostgreSQL");
    return client;
  } catch (err) {
    console.error("❌ Failed to connect to PostgreSQL:", err);
    throw err;
  }
}

async function fetchFromApi(path: string, useAgency: boolean = false): Promise<Vehicle[]> {
  if (!process.env.TRANZY_API_URL) {
    console.error("❌ TRANZY_API_URL is missing!");
    return [];
  }

  const base = process.env.TRANZY_API_URL;
  const url = path.startsWith("/") ? `${base}${path}` : `${base}/${path}`;

  console.log(`🌐 Fetching from API: ${url}`);

  const headers: Record<string, string> = {
    "X-API-KEY": process.env.TRANZY_API_KEY ?? "",
  };

  if (useAgency && process.env.TRANZY_AGENCY_ID) {
    headers["X-Agency-Id"] = process.env.TRANZY_AGENCY_ID;
    console.log(`🔑 Using Agency ID: ${process.env.TRANZY_AGENCY_ID}`);
  }

  try {
    const res = await fetch(url, { headers });
    if (!res.ok) {
      console.error(`API returned error ${res.status}`);
      return [];
    }
    const data = await res.json() as Vehicle[];
    console.log(`✅ Fetched ${Array.isArray(data) ? data.length : 1} vehicles`);
    return Array.isArray(data) ? data : [];
  } catch (err) {
    console.error("❌ Fetch error:", err);
    return [];
  }
}

async function runCronJob() {
  let client: Client | null = null;
  
  try {
    client = await createDbConnection();
    console.log(`⏱ Cron job started at ${new Date().toISOString()}`);
    
    const vehicles = await fetchFromApi("/vehicles", true);

    for (const v of vehicles) {
      const bike = v.bike_accessible === "BIKE_ACCESSIBLE";
      const wheelchair = v.wheelchair_accessible === "WHEELCHAIR_ACCESSIBLE";

      // Upsert vehicle
      await client.query(
        `INSERT INTO vehicle (id, label, vehicle_type)
         VALUES ($1,$2,$3)
         ON CONFLICT (id) DO UPDATE SET
           label = EXCLUDED.label,
           vehicle_type = EXCLUDED.vehicle_type`,
        [v.id, v.label, v.vehicle_type]
      );

      // Parse timestamp from API - handle both ISO format and "yyyy-MM-dd HH:mm:ss" format
      let ts: DateTime;
      
      // Try ISO format first (e.g., "2025-11-22T14:15:20.000Z")
      ts = DateTime.fromISO(v.timestamp);
      
      // If ISO parsing fails, try the original format (assumes Europe/Bucharest timezone)
      if (!ts.isValid) {
        ts = DateTime.fromFormat(v.timestamp, "yyyy-MM-dd HH:mm:ss", {
          zone: "Europe/Bucharest",
        });
      } else {
        // Convert ISO timestamp to Europe/Bucharest timezone if it has timezone info
        if (ts.zoneName !== "Europe/Bucharest") {
          ts = ts.setZone("Europe/Bucharest");
        }
      }
      
      if (!ts.isValid) {
        console.error("❌ Invalid timestamp for vehicle", v.id, v.timestamp);
        continue;
      }
      const parsedTimestamp = ts.toJSDate();

      // Insert vehicle state
      await client.query(
        `INSERT INTO vehicle_state (vehicle_id, latitude, longitude, timestamp, speed, bike_accessible, wheelchair_accessible)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [v.id, v.latitude, v.longitude, parsedTimestamp, v.speed ?? 0, bike, wheelchair]
      );
    }

    console.log(`✅ Cron job completed at ${new Date().toISOString()}`);

  } catch (err) {
    console.error("⚠️ Cron job error", err);
  } finally {
    if (client) {
      await client.end();
      console.log("🔒 PostgreSQL connection closed");
    }
  }
}

// Graceful shutdown handling
let intervalId: NodeJS.Timeout | null = null;
let isShuttingDown = false;

async function gracefulShutdown(signal: string) {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  
  console.log(`\n🛑 Received ${signal}, shutting down gracefully...`);
  
  if (intervalId) {
    clearInterval(intervalId);
    console.log("⏸️  Stopped cron job interval");
  }
  
  // Wait for any running cron job to complete (with timeout)
  await new Promise((resolve) => setTimeout(resolve, 5000));
  
  console.log("👋 Shutdown complete");
  process.exit(0);
}

// Setup signal handlers
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Validate environment variables before starting
validateEnvironment();

// Run once every minute using setInterval
intervalId = setInterval(runCronJob, 60_000);
runCronJob(); // run immediately once
