// Nativa Box 2.1: durable telemetry rows; retained legacy state for settings and alerts.
import {DatabaseSync} from 'node:sqlite';
import {mkdirSync} from 'node:fs';
import {defaults} from './alerts.js';
export const initial=()=>({hives:Object.fromEntries([1,2,3,4].map(n=>['NATIVA-0'+n,{enabled:n===1,settings:{...defaults},state:{},records:[],events:[]}])),users:[],sessions:[],attempts:{}});
let sqlite,pool;
const fromRow=r=>typeof r==='string'?JSON.parse(r):r;
const newer=()=>new Date().toISOString();
export async function transaction(fn){
 if(process.env.DATABASE_URL){
  const {Pool}=await import('@neondatabase/serverless');pool??=new Pool({connectionString:process.env.DATABASE_URL});
  const c=await pool.connect();
  try{
   await c.query('BEGIN');
   await c.query('CREATE TABLE IF NOT EXISTS nativa_state (id integer PRIMARY KEY, data jsonb NOT NULL)');
   await c.query(`CREATE TABLE IF NOT EXISTS telemetry_records (
     hive text NOT NULL, boot_id text NOT NULL, seq bigint NOT NULL,
     received_at timestamptz NOT NULL, record jsonb NOT NULL,
     PRIMARY KEY (hive,boot_id,seq))`);
   await c.query('CREATE INDEX IF NOT EXISTS telemetry_hive_time ON telemetry_records(hive,received_at DESC)');
   await c.query('INSERT INTO nativa_state VALUES (1,$1) ON CONFLICT DO NOTHING',[JSON.stringify(initial())]);
   const r=await c.query('SELECT data FROM nativa_state WHERE id=1 FOR UPDATE');const db=r.rows[0].data;
   if(!db.telemetryMigrated){
    for(const [hive,h] of Object.entries(db.hives))if(h.records?.length){
     await c.query(`INSERT INTO telemetry_records(hive,boot_id,seq,received_at,record)
      SELECT $1, x->>'bootId', (x->'packet'->>'seq')::bigint,
             COALESCE((x->>'receivedAt')::timestamptz,NOW()),x
      FROM jsonb_array_elements($2::jsonb) AS x
      WHERE x ? 'bootId' AND x->'packet' ? 'seq' ON CONFLICT DO NOTHING`,
      [hive,JSON.stringify(h.records)]);
    }
    db.telemetryMigrated=true;
   }
   const ctx={
    insert:async(rec)=>{const q=await c.query('INSERT INTO telemetry_records(hive,boot_id,seq,received_at,record) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING seq',[rec.hive,rec.bootId,rec.packet.seq,rec.receivedAt,JSON.stringify(rec)]);return q.rowCount===1;},
    count:async(hive)=>{const q=await c.query('SELECT count(*)::int AS n FROM telemetry_records WHERE hive=$1',[hive]);return q.rows[0].n;},
    history:async(hive,from,to,limit=320)=>{const q=await c.query(`WITH ranked AS (
      SELECT record, ntile($4::int) OVER(ORDER BY received_at,boot_id,seq) AS bucket
      FROM telemetry_records WHERE hive=$1 AND received_at >= $2 AND received_at < $3
     ) SELECT record FROM (SELECT DISTINCT ON(bucket) bucket,record FROM ranked ORDER BY bucket) t ORDER BY bucket`,[hive,from,to,limit]);return q.rows.map(x=>fromRow(x.record));},
    page:async(hive,from,to,offset,limit)=>{const q=await c.query('SELECT record FROM telemetry_records WHERE hive=$1 AND received_at >= $2 AND received_at < $3 ORDER BY received_at,boot_id,seq LIMIT $4 OFFSET $5',[hive,from,to,limit,offset]);return q.rows.map(x=>fromRow(x.record));},
   };
   const result=await fn(db,ctx);await c.query('UPDATE nativa_state SET data=$1 WHERE id=1',[JSON.stringify(db)]);await c.query('COMMIT');return result;
  }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release()}
 }
 if(process.env.VERCEL)throw Error('DATABASE_URL ausente. Configure PostgreSQL antes de ativar o serviço.');
 if(!sqlite){mkdirSync('work',{recursive:true});sqlite=new DatabaseSync('work/nativa.sqlite');sqlite.exec('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, data TEXT NOT NULL)');sqlite.prepare('INSERT OR IGNORE INTO state VALUES (1,?)').run(JSON.stringify(initial()));sqlite.exec('CREATE TABLE IF NOT EXISTS telemetry_records(hive TEXT NOT NULL,boot_id TEXT NOT NULL,seq INTEGER NOT NULL,received_at TEXT NOT NULL,record TEXT NOT NULL,PRIMARY KEY(hive,boot_id,seq))');sqlite.exec('CREATE INDEX IF NOT EXISTS telemetry_hive_time ON telemetry_records(hive,received_at)');}
 sqlite.exec('BEGIN IMMEDIATE');
 try{
  const db=JSON.parse(sqlite.prepare('SELECT data FROM state WHERE id=1').get().data);
  const stmt=sqlite.prepare('INSERT OR IGNORE INTO telemetry_records(hive,boot_id,seq,received_at,record) VALUES(?,?,?,?,?)');
  if(!db.telemetryMigrated){for(const [hive,h] of Object.entries(db.hives))for(const r of h.records||[])if(r.bootId&&Number.isInteger(r.packet?.seq))stmt.run(hive,r.bootId,r.packet.seq,r.receivedAt||newer(),JSON.stringify(r));db.telemetryMigrated=true;}
  const ctx={
   insert:async r=>stmt.run(r.hive,r.bootId,r.packet.seq,r.receivedAt,JSON.stringify(r)).changes===1,
   count:async hive=>sqlite.prepare('SELECT count(*) AS n FROM telemetry_records WHERE hive=?').get(hive).n,
   history:async(hive,from,to,limit=320)=>{const rows=sqlite.prepare('SELECT record FROM telemetry_records WHERE hive=? AND received_at>=? AND received_at<? ORDER BY received_at,boot_id,seq').all(hive,from,to).map(x=>JSON.parse(x.record));if(rows.length<=limit)return rows;return Array.from({length:limit},(_,i)=>rows[Math.min(rows.length-1,Math.floor(i*(rows.length-1)/(limit-1)))]);},
   page:async(hive,from,to,offset,limit)=>sqlite.prepare('SELECT record FROM telemetry_records WHERE hive=? AND received_at>=? AND received_at<? ORDER BY received_at,boot_id,seq LIMIT ? OFFSET ?').all(hive,from,to,limit,offset).map(x=>JSON.parse(x.record)),
  };
  const result=await fn(db,ctx);sqlite.prepare('UPDATE state SET data=? WHERE id=1').run(JSON.stringify(db));sqlite.exec('COMMIT');return result;
 }catch(e){sqlite.exec('ROLLBACK');throw e;}
}
