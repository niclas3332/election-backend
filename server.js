// server.js
import express from 'express';
import pg from 'pg';
import cors from 'cors';
import cron from 'node-cron';
import axios from 'axios';
import Papa from 'papaparse';
import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import dotenv from 'dotenv';
import moment from "moment"
dotenv.config();

// Configure PostgreSQL numeric type parser
pg.types.setTypeParser(pg.types.builtins.NUMERIC, val => parseFloat(val));
pg.types.setTypeParser(pg.types.builtins.INT8, val => parseInt(val));

const corsOptions = {
    origin: process.env.CORS_ORIGIN,
    optionsSuccessStatus: 200
}

const app = express();
app.use(cors(corsOptions));
app.use(express.json());

// PostgreSQL Connection Pool
const pool = new pg.Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT),
});

async function calculateSHA256(content) {
    const hash = crypto.createHash('sha256');
    hash.update(content);
    return hash.digest('hex');
}

async function fileWithHashExists(hash) {
    try {
        const csvDir = path.join(process.cwd(), 'election_data');
        const files = await fs.readdir(csvDir);
        for (const file of files) {
            if (file.includes(hash)) {
                return true;
            }
        }
        return false;
    } catch (error) {
        if (error.code === 'ENOENT') {
            return false;
        }
        throw error;
    }
}

function formatTimestampForFilename(date) {
    return date.toISOString().replace(/[:.]/g, '-');
}

async function isNewData(lastUpdated) {
    const result = await pool.query(
        'SELECT last_updated FROM snapshots ORDER BY timestamp DESC LIMIT 1'
    );

    if (result.rows.length === 0) return true;

    const lastStoredUpdate = result.rows[0].last_updated;
    return lastUpdated.getTime() !== lastStoredUpdate.getTime();
}

// Helper function to parse German number format (e.g., "1.234,56" -> 1234.56)
function parseGermanNumber(str) {
    if (!str) return 0;
    return parseFloat(str.replace(/\./g, '').replace(',', '.'));
}

// Database initialization
async function initDatabase() {
    const client = await pool.connect();
    try {
        await client.query(`
            CREATE TABLE IF NOT EXISTS snapshots (
                                                     id SERIAL PRIMARY KEY,
                                                     timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                     status TEXT,
                                                     last_updated TIMESTAMP WITH TIME ZONE
            );

            CREATE TABLE IF NOT EXISTS federal_results (
                                                           id SERIAL PRIMARY KEY,
                                                           snapshot_id INTEGER REFERENCES snapshots(id),
                                                           eligible_voters BIGINT,
                                                           total_voters BIGINT,
                                                           turnout_percentage NUMERIC(5,2),
                                                           invalid_primary BIGINT,
                                                           invalid_secondary BIGINT,
                                                           valid_primary BIGINT,
                                                           valid_secondary BIGINT
            );

            CREATE TABLE IF NOT EXISTS federal_party_results (
                                                                 id SERIAL PRIMARY KEY,
                                                                 snapshot_id INTEGER REFERENCES snapshots(id),
                                                                 party_name TEXT,
                                                                 percentage NUMERIC(5,2),
                                                                 votes BIGINT
            );

            CREATE TABLE IF NOT EXISTS state_results (
                                                         id SERIAL PRIMARY KEY,
                                                         snapshot_id INTEGER REFERENCES snapshots(id),
                                                         state_number TEXT,
                                                         state_name TEXT,
                                                         eligible_voters BIGINT,
                                                         total_voters BIGINT,
                                                         turnout_percentage NUMERIC(5,2),
                                                         invalid_primary BIGINT,
                                                         invalid_secondary BIGINT,
                                                         valid_primary BIGINT,
                                                         valid_secondary BIGINT
            );

            CREATE TABLE IF NOT EXISTS state_party_results (
                                                               id SERIAL PRIMARY KEY,
                                                               snapshot_id INTEGER REFERENCES snapshots(id),
                                                               state_number TEXT,
                                                               party_name TEXT,
                                                               percentage NUMERIC(5,2),
                                                               votes BIGINT
            );

            CREATE TABLE IF NOT EXISTS district_results (
                                                            id SERIAL PRIMARY KEY,
                                                            snapshot_id INTEGER REFERENCES snapshots(id),
                                                            district_number TEXT,
                                                            district_name TEXT,
                                                            state_number TEXT,
                                                            eligible_voters BIGINT,
                                                            total_voters BIGINT,
                                                            turnout_percentage NUMERIC(5,2),
                                                            invalid_primary BIGINT,
                                                            invalid_secondary BIGINT,
                                                            valid_primary BIGINT,
                                                            valid_secondary BIGINT
            );

            CREATE TABLE IF NOT EXISTS district_party_results (
                                                                  id SERIAL PRIMARY KEY,
                                                                  snapshot_id INTEGER REFERENCES snapshots(id),
                                                                  district_number TEXT,
                                                                  party_name TEXT,
                                                                  percentage NUMERIC(5,2),
                                                                  votes BIGINT
            );
        `);
    } finally {
        client.release();
    }
}

// Extract metadata from CSV header
function extractMetadata(lines) {
    const statusLine = lines[4].split(';');
    const timestampLine = lines[5].split(';');

    console.log(`${timestampLine[1]} ${timestampLine[2]}`.trim())
    return {
        status: statusLine[1]?.trim() || '',
        lastUpdated: moment(`${timestampLine[1]} ${timestampLine[2]}`.trim(), "DD.MM.YY HH:mm:ss").toDate()
    };
}

// Process system group data (voters, turnout, etc.)
function processSystemGroup(row) {
    if (row.Gruppenart !== 'System-Gruppe') return {};

    const result = {};

    switch (row.Gruppenname) {
        case 'Wahlberechtigte':
            result.eligible_voters = parseGermanNumber(row.Anzahl);
            break;
        case 'Wählende':
            result.total_voters = parseGermanNumber(row.Anzahl);
            result.turnout_percentage = parseGermanNumber(row.Prozent);
            break;
        case 'Ungültige':
            if (row.Stimme === '1') {
                result.invalid_primary = parseGermanNumber(row.Anzahl);
            } else if (row.Stimme === '2') {
                result.invalid_secondary = parseGermanNumber(row.Anzahl);
            }
            break;
        case 'Gültige':
            if (row.Stimme === '1') {
                result.valid_primary = parseGermanNumber(row.Anzahl);
            } else if (row.Stimme === '2') {
                result.valid_secondary = parseGermanNumber(row.Anzahl);
            }
            break;
    }

    return result;
}

// Process party results
function processPartyResult(row) {
    if (row.Gruppenart !== 'Partei' || row.Stimme !== '2') return null;

    return {
        partyName: row.Gruppenname,
        percentage: parseGermanNumber(row.Prozent),
        votes: parseGermanNumber(row.Anzahl)
    };
}

// Modify fetchElectionData to save CSV
async function fetchElectionData() {
    try {
        const response = await axios.get(process.env.ELECTION_CSV_URL, {
            responseType: 'arraybuffer'
        });

        const text = new TextDecoder('utf-8').decode(response.data);
        const lines = text.split('\n');
        const metadata = extractMetadata(lines);

        // Calculate hash and save CSV to filesystem if new
        const fileHash = await calculateSHA256(text);
        const hashExists = await fileWithHashExists(fileHash);

        if (!hashExists) {
            const timestamp = formatTimestampForFilename(new Date());
            const csvDir = path.join(process.cwd(), 'election_data');
            await fs.mkdir(csvDir, { recursive: true });
            await fs.writeFile(
                path.join(csvDir, `election_data_${timestamp}_${fileHash}.csv`),
                text
            );
            console.log(`Saved new CSV with hash: ${fileHash}`);
        } else {
            console.log(`CSV with hash ${fileHash} already exists, skipping save`);
        }

        return new Promise((resolve, reject) => {
            Papa.parse(text, {
                delimiter: ';',
                header: true,
                skipEmptyLines: true,
                transformHeader: h => h.trim(),
                beforeFirstChunk: chunk => {
                    const lines = chunk.split('\n');
                    return lines[9] + '\n' + lines.slice(10).join('\n');
                },
                complete: results => resolve({ data: results.data, metadata, csvText: text }),
                error: error => reject(error)
            });
        });
    } catch (error) {
        console.error('Error fetching data:', error);
        throw error;
    }
}

// Store election data in database
async function storeElectionData({ data, metadata }) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        console.log(metadata)
        // Create new snapshot
        const snapshotResult = await client.query(
            'INSERT INTO snapshots (status, last_updated) VALUES ($1, $2) RETURNING id',
            [metadata.status, metadata.lastUpdated]
        );
        const snapshotId = snapshotResult.rows[0].id;

        // Process federal results
        const federalResults = {
            eligible_voters: 0, total_voters: 0, turnout_percentage: 0,
            invalid_primary: 0, invalid_secondary: 0, valid_primary: 0, valid_secondary: 0
        };
        const federalPartyResults = [];
        const stateResults = new Map();
        const statePartyResults = new Map();
        const districtResults = new Map();
        const districtPartyResults = new Map();

        for (const row of data) {
            if (row.Gebietsart === 'Bund') {
                if (row.Gruppenart === 'System-Gruppe') {
                    const systemData = processSystemGroup(row);
                    Object.entries(systemData).forEach(([key, value]) => {
                        if (value !== null) federalResults[key.toLowerCase()] = value;
                    });
                } else if (row.Gruppenart === 'Partei') {
                    const partyResult = processPartyResult(row);
                    if (partyResult) federalPartyResults.push(partyResult);
                }
            } else if (row.Gebietsart === 'Wahlkreis') {
                const districtNumber = row.Gebietsnummer;
                const stateNumber = row.UegGebietsnummer;

                if (!districtResults.has(districtNumber)) {
                    districtResults.set(districtNumber, {
                        district_number: districtNumber,
                        district_name: row.Gebietsname,
                        state_number: stateNumber,
                        eligible_voters: 0, total_voters: 0, turnout_percentage: 0,
                        invalid_primary: 0, invalid_secondary: 0, valid_primary: 0, valid_secondary: 0
                    });
                }

                if (row.Gruppenart === 'System-Gruppe') {
                    const systemData = processSystemGroup(row);
                    Object.entries(systemData).forEach(([key, value]) => {
                        if (value !== null) districtResults.get(districtNumber)[key.toLowerCase()] = value;
                    });
                } else if (row.Gruppenart === 'Partei') {
                    const partyResult = processPartyResult(row);
                    if (partyResult) {
                        if (!districtPartyResults.has(districtNumber)) {
                            districtPartyResults.set(districtNumber, []);
                        }
                        districtPartyResults.get(districtNumber).push(partyResult);
                    }
                }
            } else if (row.Gebietsart === 'Land') {
                const stateNumber = row.Gebietsnummer;
                if (!stateResults.has(stateNumber)) {
                    stateResults.set(stateNumber, {
                        state_number: stateNumber,
                        state_name: row.Gebietsname,
                        eligible_voters: 0, total_voters: 0, turnout_percentage: 0,
                        invalid_primary: 0, invalid_secondary: 0, valid_primary: 0, valid_secondary: 0
                    });
                }

                if (row.Gruppenart === 'System-Gruppe') {
                    const systemData = processSystemGroup(row);
                    Object.entries(systemData).forEach(([key, value]) => {
                        if (value !== null) stateResults.get(stateNumber)[key.toLowerCase()] = value;
                    });
                } else if (row.Gruppenart === 'Partei') {
                    const partyResult = processPartyResult(row);
                    if (partyResult) {
                        if (!statePartyResults.has(stateNumber)) {
                            statePartyResults.set(stateNumber, []);
                        }
                        statePartyResults.get(stateNumber).push(partyResult);
                    }
                }
            }
        }

        // Store federal results
        await client.query(
            `INSERT INTO federal_results
             (snapshot_id, eligible_voters, total_voters, turnout_percentage,
              invalid_primary, invalid_secondary, valid_primary, valid_secondary)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [snapshotId, federalResults.eligible_voters, federalResults.total_voters,
                federalResults.turnout_percentage, federalResults.invalid_primary,
                federalResults.invalid_secondary, federalResults.valid_primary,
                federalResults.valid_secondary]
        );

        // Store federal party results
        for (const party of federalPartyResults) {
            await client.query(
                'INSERT INTO federal_party_results (snapshot_id, party_name, percentage, votes) VALUES ($1, $2, $3, $4)',
                [snapshotId, party.partyName, party.percentage, party.votes]
            );
        }

        // Store state results
        for (const state of stateResults.values()) {
            await client.query(
                `INSERT INTO state_results
                 (snapshot_id, state_number, state_name, eligible_voters, total_voters,
                  turnout_percentage, invalid_primary, invalid_secondary, valid_primary, valid_secondary)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
                [snapshotId, state.state_number, state.state_name, state.eligible_voters,
                    state.total_voters, state.turnout_percentage, state.invalid_primary,
                    state.invalid_secondary, state.valid_primary, state.valid_secondary]
            );
        }

// Store state party results
        for (const [stateNumber, parties] of statePartyResults) {
            for (const party of parties) {
                await client.query(
                    'INSERT INTO state_party_results (snapshot_id, state_number, party_name, percentage, votes) VALUES ($1, $2, $3, $4, $5)',
                    [snapshotId, stateNumber, party.partyName, party.percentage, party.votes]
                );
            }
        }

        // Store district results
        for (const district of districtResults.values()) {
            await client.query(
                `INSERT INTO district_results
                 (snapshot_id, district_number, district_name, state_number, eligible_voters,
                  total_voters, turnout_percentage, invalid_primary, invalid_secondary,
                  valid_primary, valid_secondary)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
                [snapshotId, district.district_number, district.district_name, district.state_number,
                    district.eligible_voters, district.total_voters, district.turnout_percentage,
                    district.invalid_primary, district.invalid_secondary, district.valid_primary,
                    district.valid_secondary]
            );
        }

        // Store district party results
        for (const [districtNumber, parties] of districtPartyResults) {
            for (const party of parties) {
                await client.query(
                    'INSERT INTO district_party_results (snapshot_id, district_number, party_name, percentage, votes) VALUES ($1, $2, $3, $4, $5)',
                    [snapshotId, districtNumber, party.partyName, party.percentage, party.votes]
                );
            }
        }

        await client.query('COMMIT');
        return snapshotId;
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        client.release();
    }
}

app.get('/api/latest-csv', async (req, res) => {
    try {
        const csvDir = path.join(process.cwd(), 'election_data');
        const files = await fs.readdir(csvDir);

        // Filter for CSV files and sort by timestamp in filename
        const csvFiles = files
            .filter(file => file.endsWith('.csv'))
            .sort((a, b) => {
                // Extract timestamps from filenames (between 'data_' and '_hash')
                const getTimestamp = filename => {
                    const match = filename.match(/data_(.+?)_[a-f0-9]{64}/);
                    return match ? match[1] : '';
                };
                return getTimestamp(b).localeCompare(getTimestamp(a));
            });

        if (csvFiles.length === 0) {
            return res.status(404).json({ error: 'No CSV files found' });
        }

        const latestFile = csvFiles[0];
        const filePath = path.join(csvDir, latestFile);
        const fileContent = await fs.readFile(filePath, 'utf-8');

        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="latest_election_data.csv"`);
        res.send(fileContent);
    } catch (error) {
        console.error('Error serving CSV:', error);
        res.status(500).json({ error: error.message });
    }
});

// API Routes
app.get('/api/federal/current', async (req, res) => {
    try {
        const result = await pool.query(`
            WITH latest_snapshot AS (
                SELECT id FROM snapshots ORDER BY timestamp DESC LIMIT 1
            )
            SELECT
                s.timestamp,
                s.status,
                s.last_updated,
                fr.*,
                json_agg(json_build_object(
                        'party_name', fpr.party_name,
                        'percentage', fpr.percentage,
                        'votes', fpr.votes
                         ) ORDER BY fpr.percentage DESC) as parties
            FROM latest_snapshot ls
                     JOIN snapshots s ON s.id = ls.id
                     JOIN federal_results fr ON fr.snapshot_id = ls.id
                     JOIN federal_party_results fpr ON fpr.snapshot_id = ls.id
            GROUP BY s.timestamp, s.status, s.last_updated, fr.id
        `);
        res.json(result.rows[0]);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/federal/history', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT
                s.id as snapshot_id,
                s.timestamp,
                s.status,
                s.last_updated,
                fr.eligible_voters,
                fr.total_voters,
                fr.turnout_percentage,
                fr.invalid_primary,
                fr.invalid_secondary,
                fr.valid_primary,
                fr.valid_secondary,
                json_agg(json_build_object(
                        'party_name', fpr.party_name,
                        'percentage', fpr.percentage,
                        'votes', fpr.votes
                         ) ORDER BY fpr.percentage DESC) as parties
            FROM snapshots s
                     JOIN federal_results fr ON fr.snapshot_id = s.id
                     JOIN federal_party_results fpr ON fpr.snapshot_id = s.id
            GROUP BY
                s.id,
                s.timestamp,
                s.status,
                s.last_updated,
                fr.eligible_voters,
                fr.total_voters,
                fr.turnout_percentage,
                fr.invalid_primary,
                fr.invalid_secondary,
                fr.valid_primary,
                fr.valid_secondary
            ORDER BY s.timestamp DESC
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});
app.get('/api/districts/history', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT
                s.id as snapshot_id,
                s.timestamp,
                s.status,
                s.last_updated,
                dr.district_number,
                dr.district_name,
                dr.state_number,
                dr.eligible_voters,
                dr.total_voters,
                dr.turnout_percentage,
                dr.invalid_primary,
                dr.invalid_secondary,
                dr.valid_primary,
                dr.valid_secondary,
                json_agg(json_build_object(
                        'party_name', dpr.party_name,
                        'percentage', dpr.percentage,
                        'votes', dpr.votes
                         ) ORDER BY dpr.percentage DESC) as parties
            FROM snapshots s
                     JOIN district_results dr ON dr.snapshot_id = s.id
                     JOIN district_party_results dpr ON dpr.snapshot_id = s.id
                     AND dpr.district_number = dr.district_number
            GROUP BY
                s.id,
                s.timestamp,
                s.status,
                s.last_updated,
                dr.district_number,
                dr.district_name,
                dr.state_number,
                dr.eligible_voters,
                dr.total_voters,
                dr.turnout_percentage,
                dr.invalid_primary,
                dr.invalid_secondary,
                dr.valid_primary,
                dr.valid_secondary
            ORDER BY s.timestamp DESC, dr.district_number
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});
app.get('/api/districts/current', async (req, res) => {
    try {
        const result = await pool.query(`
            WITH latest_snapshot AS (
                SELECT id FROM snapshots ORDER BY timestamp DESC LIMIT 1
            )
            SELECT
                dr.district_number,
                dr.district_name,
                dr.state_number,
                dr.eligible_voters,
                dr.total_voters,
                dr.turnout_percentage,
                dr.invalid_primary,
                dr.invalid_secondary,
                dr.valid_primary,
                dr.valid_secondary,
                json_agg(json_build_object(
                        'party_name', dpr.party_name,
                        'percentage', dpr.percentage,
                        'votes', dpr.votes
                         ) ORDER BY dpr.percentage DESC) as parties
            FROM latest_snapshot ls
                     JOIN district_results dr ON dr.snapshot_id = ls.id
                     JOIN district_party_results dpr ON dpr.snapshot_id = ls.id
                AND dpr.district_number = dr.district_number
            GROUP BY dr.id
            ORDER BY dr.district_number
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/states/current', async (req, res) => {
    try {
        const result = await pool.query(`
            WITH latest_snapshot AS (
                SELECT id FROM snapshots ORDER BY timestamp DESC LIMIT 1
            )
            SELECT
                sr.state_number,
                sr.state_name,
                sr.eligible_voters,
                sr.total_voters,
                sr.turnout_percentage,
                sr.invalid_primary,
                sr.invalid_secondary,
                sr.valid_primary,
                sr.valid_secondary,
                json_agg(json_build_object(
                        'party_name', spr.party_name,
                        'percentage', spr.percentage,
                        'votes', spr.votes
                         ) ORDER BY spr.percentage DESC) as parties
            FROM latest_snapshot ls
                     JOIN state_results sr ON sr.snapshot_id = ls.id
                     JOIN state_party_results spr ON spr.snapshot_id = ls.id
                AND spr.state_number = sr.state_number
            GROUP BY sr.id
            ORDER BY sr.state_number
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/fetch-now', async (req, res) => {
    try {
        const { data, metadata } = await fetchElectionData();
        const isNew = await isNewData(metadata.lastUpdated);

        if (isNew) {
            const snapshotId = await storeElectionData({ data, metadata });
            res.json({ success: true, snapshotId, dataUpdated: true });
        } else {
            res.json({ success: true, dataUpdated: false });
        }
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Schedule data collection
cron.schedule('* * * * *', async () => {
    try {
        console.log('Starting scheduled data fetch:', new Date().toISOString());
        const { data, metadata } = await fetchElectionData();

        // Check if we have new data
        const isNew = await isNewData(metadata.lastUpdated);

        if (isNew) {
            console.log('New data detected, storing in database');
            await storeElectionData({ data, metadata });
            console.log('Completed storing new data');
        } else {
            console.log('No new data detected, skipping database update');
        }
    } catch (error) {
        console.error('Error in scheduled task:', error);
    }
});

// Start server
initDatabase()
    .then(() => {
        const PORT = process.env.PORT || 3010;
        app.listen(PORT, () => {
            console.log(`Server running on port ${PORT}`);
        });
    })
    .catch(error => {
        console.error('Failed to initialize database:', error);
        process.exit(1);
    })