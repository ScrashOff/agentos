import {pool} from '../server/db.js';
try{
 const id='3ccc273d-e790-43a8-ba86-82c1287907c5';
 console.log(JSON.stringify({policy:(await pool.query("SELECT value FROM settings WHERE id=$1",['workforce:'+id])).rows[0]?.value,states:(await pool.query("SELECT t.execution->>'workforceLane' lane,t.execution->>'kind' runtime,t.state,count(*)::int n FROM tasks t WHERE goal_id=$1 GROUP BY 1,2,3 ORDER BY 1,2,3",[id])).rows,remaining:(await pool.query("SELECT t.id,t.key,t.title,t.state,t.stage,t.model,t.reason,t.execution->>'baseWorkspace' base,t.execution->>'workforceLane' lane FROM tasks t WHERE goal_id=$1 AND state NOT IN('accepted','superseded','cancelled') ORDER BY key",[id])).rows}));
}finally{await pool.end();}
