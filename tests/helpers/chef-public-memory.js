export function chefPublicMemoryModel(seed=[]){
  const docs=structuredClone(seed);let sequence=0;
  const copy=value=>value==null?value:structuredClone(value);
  const matches=(doc,filter)=>Object.entries(filter).every(([key,value])=>{
    if(value&&typeof value==='object'&&!(value instanceof Date)){
      if('$in'in value)return value.$in.some(v=>String(doc[key])===String(v));
      if('$gt'in value)return new Date(doc[key])>new Date(value.$gt);
    }
    return (doc[key]==null&&value==null)||String(doc[key])===String(value);
  });
  const chain=initial=>{let value=initial;const query={then:(resolve,reject)=>Promise.resolve(copy(value)).then(resolve,reject),lean:async()=>copy(value),select:()=>query,sort:sort=>{if(Array.isArray(value))value=value.slice().sort((a,b)=>{for(const [key,order]of Object.entries(sort)){if(a[key]>b[key])return order;if(a[key]<b[key])return -order;}return 0;});return query;},limit:n=>{if(Array.isArray(value))value=value.slice(0,n);return query;}};return query;};
  function update(filter,ops,options={}){let doc=docs.find(d=>matches(d,filter));if(!doc&&options.upsert){doc={_id:filter._id||String(++sequence).padStart(24,'0'),...copy(filter),...copy(ops.$setOnInsert||{})};docs.push(doc);}if(!doc)return null;if(ops.$set)Object.assign(doc,copy(ops.$set));for(const [key,value]of Object.entries(ops.$inc||{}))doc[key]=(doc[key]||0)+value;for(const [key,value]of Object.entries(ops.$push||{})){doc[key]=[...(doc[key]||[]),...copy(value.$each)];if(value.$slice)doc[key]=doc[key].slice(value.$slice);}return copy(doc);}
  return {docs,async create(value){if(docs.some(d=>String(d._id)===String(value._id)))throw Object.assign(Error('duplicate'),{code:11000});const doc={_id:String(++sequence).padStart(24,'0'),...copy(value)};docs.push(doc);return copy(doc);},findOne:filter=>chain(docs.find(d=>matches(d,filter))||null),find:filter=>chain(docs.filter(d=>matches(d,filter))),updateOne:async(filter,ops,options)=>{update(filter,ops,options);return{acknowledged:true};},findOneAndUpdate:async(filter,ops,options)=>update(filter,ops,options)};
}
