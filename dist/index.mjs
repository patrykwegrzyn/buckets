import{EventEmitter as p}from"events";import{open as f}from"lmdbx";var d=class extends p{constructor(e,n){super();this.dbs=new Map;this.flushing=!1;this.indexes=new Map;this.env=f(e,n),this.ttlBucket=this.env.openDB("ttl",{cache:!0}),this.indexDb=this.env.openDB("index",{cache:!0})}ttlKey(e,n,r){return`${e}:${n}:${r}`}async committed(){return this.env.committed}wrapDB(e,n){let r=Object.create(e);return r.query=async(t,i)=>{let s=[],o=this.indexKey(n,t,i);for(let{value:a}of this.indexDb.getRange({start:o,end:`${o}\xFF`}))s.push(a);return(await e.getMany(s)).filter(a=>a!==void 0)},r.put=(t,i,s)=>{let o;if(s?.version!==void 0&&s.ifVersion!==void 0?o=e.put(t,i,s.version,s.ifVersion):s?.version!==void 0?o=e.put(t,i,s.version):o=e.put(t,i),s?.ttl){let a=Date.now()+s.ttl,u=this.ttlKey(a,n,String(t));this.ttlBucket.put(u,"")}let c=this.indexes.get(n);if(c)for(let a of c){let u=i[a],l=this.indexKey(n,a,u,t);this.indexDb.put(l,String(t))}return s?.quiet||this.emit("change",{op:"put",bucket:n,id:t,value:e.encoder.encode(i),version:s?.version,ttl:s?.ttl}),o},r.remove=(t,i)=>{let s=e.get(t),o=this.indexes.get(n);if(o&&s)for(let c of o){let a=s[c],u=this.indexKey(n,c,a,t);this.indexDb.remove(u)}return i?.quiet||this.emit("change",{op:"remove",bucket:n,id:t,value:s,version:i?.ifVersion}),i?.ifVersion!==void 0?e.remove(t,i.ifVersion):e.remove(t)},r}indexKey(e,n,r,t){return[e,n,r,t].filter(i=>i!==void 0).join(":")}bucket(e,n){let r=this.dbs.get(e);if(!r){let t={cache:!0,...n},i=this.env.openDB(e,t);r=this.wrapDB(i,e),n?.indexes&&this.indexes.set(e,n.indexes),this.dbs.set(e,r)}return r}async clean(){if(this.flushing)return;this.flushing=!0;let e=[],n=Date.now().toString();for(let r of this.ttlBucket.getKeys({end:n})){let t=r.split(":");if(t.length<3)continue;let i=t[1],s=t.slice(2).join(":");e.push({ttlKey:r,bucketName:i,id:s})}await this.env.transaction(()=>{for(let{ttlKey:r,bucketName:t,id:i}of e)this.bucket(t).remove(i,{quiet:!0}),this.ttlBucket.remove(r)}),this.flushing=!1}async close(){let e=Array.from(this.dbs.values()).map(n=>n.close());await Promise.all([...e,this.env.close(),this.ttlBucket.close()])}};export{d as Store};
//# sourceMappingURL=index.mjs.map