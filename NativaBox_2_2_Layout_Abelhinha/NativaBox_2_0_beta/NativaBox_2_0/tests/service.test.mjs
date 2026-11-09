import test from 'node:test';import assert from 'node:assert/strict';import {mkdtempSync,mkdirSync} from 'node:fs';import {randomBytes} from 'node:crypto';import path from 'node:path';
mkdirSync('work',{recursive:true});process.chdir(mkdtempSync(path.resolve('work/test-')));
process.env.ADMIN_EMAIL='admin@example.test';process.env.ADMIN_PASSWORD=randomBytes(20).toString('hex');process.env.INGEST_TOKEN=randomBytes(24).toString('hex');process.env.CRON_SECRET=randomBytes(24).toString('hex');delete process.env.DATABASE_URL;delete process.env.VERCEL;
const {service}=await import('../lib/service.js');
const call=(p,b={},headers={},method='POST')=>service({method,path:new URL(p,'http://local').pathname,url:p,headers,body:b});
test('autenticação, ingestão, duplicatas, histórico, configuração e permissões',async()=>{
 await assert.rejects(call('/api/state',{}, {},'GET'),{status:401});
 const login=await call('/api/login',{email:process.env.ADMIN_EMAIL,password:process.env.ADMIN_PASSWORD});assert.equal(login.user.role,'admin');const headers={cookie:login.cookie.split(';')[0]};
 const packet={v:'1.2',seq:1,ti:25,hi:60,te:20,he:62,bat_v:12.6,massa_g:100,status:[0,0,3,3,3,4,0]};const payload={hive:'NATIVA-01',bootId:'test-boot',simulated:false,packet};
 await assert.rejects(call('/api/ingest',payload),{status:401});
 const gateway={authorization:'Bearer '+process.env.INGEST_TOKEN};assert.equal((await call('/api/ingest',payload,gateway)).accepted,true);assert.equal((await call('/api/ingest',payload,gateway)).duplicate,true);
 await assert.rejects(call('/api/ingest',{...payload,hive:'NATIVA-02'},gateway),{status:409});
 let data=await call('/api/state',{},headers,'GET');assert.equal(data.hives['NATIVA-01'].records.length,1);assert.equal(data.hives['NATIVA-01'].records[0].packet.massa_g,null);
 await call('/api/settings',{hive:'NATIVA-01',enabled:true,settings:{...data.hives['NATIVA-01'].settings,enabled:true}},headers);
 await call('/api/ingest',{...payload,packet:{...packet,seq:2,bat_v:10.5}},gateway);data=await call('/api/state',{},headers,'GET');assert.equal(data.hives['NATIVA-01'].state.battery.active,true);
 await call('/api/users',{email:'viewer@example.test',password:'test-only-password-12345',role:'viewer',enabled:true},headers);
 const viewer=await call('/api/login',{email:'viewer@example.test',password:'test-only-password-12345'});const vh={cookie:viewer.cookie.split(';')[0]};await assert.rejects(call('/api/settings',{hive:'NATIVA-01'},vh),{status:403});
 await assert.rejects(call('/api/settings',{}, {...headers,origin:'https://outra.example'}),{status:403});
 const now=Date.now();const from=new Date(now-86400000).toISOString(),to=new Date(now+60000).toISOString();
 const hist=await call('/api/history?'+new URLSearchParams({hive:'NATIVA-01',from,to}),{},headers,'GET');
 assert.equal(hist.records.length,2);assert.equal(hist.records[0].packet.massa_g,null);
 const exportData=await call('/api/export?'+new URLSearchParams({hive:'NATIVA-01',from,to,offset:0}),{},headers,'GET');
 assert.match(exportData.csv,/received_at,source,session,seq/);assert.equal(exportData.nextOffset,null);
 assert.equal(data.recordCounts['NATIVA-01'],2);
 await assert.rejects(call('/api/history?hive=NATIVA-01&from=2020-01-01T00:00:00.000Z&to=2020-03-01T00:00:00.000Z',{},headers,'GET'),{status:400});
 const {transaction}=await import('../lib/store.js');
 await transaction(async (_db,t)=>{for(let i=10;i<1015;i++){
  const r={hive:'NATIVA-01',bootId:'long-term-test',receivedAt:new Date(Date.now()-2000000+i*1000).toISOString(),packet:{...packet,seq:i,massa_g:null},source:'Gateway',session:'long-term-test',simulated:false};
  assert.equal(await t.insert(r),true);
 }});
 const afterward=await call('/api/state',{},headers,'GET');
 assert.equal(afterward.recordCounts['NATIVA-01'],1007);
 assert.equal(afterward.hives['NATIVA-01'].records.length,2); // state cache is independent of durable history
 const longHistory=await call('/api/history?'+new URLSearchParams({hive:'NATIVA-01',from,to}),{},headers,'GET');
 assert.ok(longHistory.records.length<=320);
 const exportLong=await call('/api/export?'+new URLSearchParams({hive:'NATIVA-01',from,to,offset:0}),{},headers,'GET');
 assert.equal(exportLong.csv.split('\r\n').length,1008); // header + 1007 readings

 await call('/api/logout',{},headers);await assert.rejects(call('/api/state',{},headers,'GET'),{status:401});
});
