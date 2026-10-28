export function chefMemoryModel() {
 const docs=[];let next=1;
 const copy=v=>v==null?v:structuredClone(v);
 const matches=(doc,filter)=>Object.entries(filter).every(([k,v])=>String(doc[k])===String(v));
 const chain=value=>{const q={then:(resolve,reject)=>Promise.resolve(copy(value)).then(resolve,reject),lean:async()=>copy(value),select:()=>q,sort:()=>q,limit:()=>q};return q;};
 function update(filter,ops,options={}){let doc=docs.find(d=>matches(d,filter));if(!doc&&options.upsert){doc={_id:String(next++),...filter,...copy(ops.$setOnInsert||{})};docs.push(doc);}if(!doc)return null;if(ops.$set)Object.assign(doc,copy(ops.$set));for(const[k,v]of Object.entries(ops.$inc||{}))doc[k]=(doc[k]||0)+v;for(const[k,v]of Object.entries(ops.$push||{})){doc[k]=[...(doc[k]||[]),...copy(v.$each)];if(v.$slice)doc[k]=doc[k].slice(v.$slice);}return copy(doc);}
 return {docs,updateOne:async(filter,ops,options)=>{update(filter,ops,options);return {acknowledged:true};},findOne:filter=>chain(docs.find(d=>matches(d,filter))||null),find:filter=>chain(docs.filter(d=>matches(d,filter))),findOneAndUpdate:async(filter,ops,options)=>update(filter,ops,options)};
}
