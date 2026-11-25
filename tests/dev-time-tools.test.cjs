const test=require("node:test"),assert=require("node:assert/strict");
const cron=require("../tools/cron-builder-core.js"),date=require("../tools/date-time-converter-core.js");
test("Cron ranges, steps, Sunday alias and validation",()=>{
 assert.equal(cron.next("*/15 * * * *",new Date("2026-11-25T12:00:00Z"),1)[0].toISOString(),"2026-11-25T12:15:00.000Z");
 assert.equal(cron.next("0 0 * * 7",new Date("2026-11-25T12:00:00Z"),1)[0].toISOString(),"2026-11-29T00:00:00.000Z");
 assert.throws(()=>cron.parse("60 * * * *"));assert.throws(()=>cron.parse("*/0 * * * *"));
 assert.equal(cron.next("0 0 1 * 1",new Date("2026-11-25T12:00:00Z"),1)[0].toISOString(),"2026-11-30T00:00:00.000Z");
});
test("Date conversion and strict input validation",()=>{
 assert.equal(date.parse("0","seconds").toISOString(),"1970-01-01T00:00:00.000Z");
 assert.equal(date.parse("1001","milliseconds").getTime(),1001);
 assert.equal(date.parse("2026-11-25T12:00:00+01:00","iso").toISOString(),"2026-11-25T11:00:00.000Z");
 assert.throws(()=>date.parse("2026-02-30T12:00Z","iso"));
 assert.throws(()=>date.parse("2026-11-25T12:00","iso"));
 assert.match(date.format(new Date("2026-07-01T12:00Z"),"Europe/Luxembourg"),/14:00:00/);
});
