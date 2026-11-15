import {api} from './client.js';
import {session} from '../stores/session.js';
export const datasets={list:()=>api('/datasets'),get:id=>api('/datasets/'+id),create:data=>api('/datasets',{method:'POST',body:JSON.stringify(data)})};
export const jobs={list:()=>api('/jobs'),get:id=>api('/jobs/'+id),create:data=>api('/jobs',{method:'POST',body:JSON.stringify(data)}),approve:id=>api('/jobs/'+id+'/approve',{method:'POST'})};
export const reports={list:()=>api('/reports'),create:data=>api('/reports',{method:'POST',body:JSON.stringify(data)}),toggle:id=>api('/reports/'+id+'/toggle',{method:'POST'}),deliveries:()=>api('/deliveries')};
export const handovers={
  list:()=>api('/handovers'),
  get:id=>api('/handovers/'+id),
  export:dataset_id=>api('/handovers/export',{method:'POST',body:JSON.stringify({dataset_id})}),
  import:async file=>{
    const r=await fetch('/api/handovers/import?filename='+encodeURIComponent(file.name),{method:'POST',headers:{'Content-Type':'application/zip',...(session.token?{Authorization:'Bearer '+session.token}:{})},body:await file.arrayBuffer()});
    const data=await r.json().catch(()=>({detail:'导入失败，请稍后重试'}));
    if(!r.ok)throw new Error(typeof data.detail==='string'?data.detail:JSON.stringify(data.detail));
    return data;
  },
  async download(id){
    const r=await fetch('/api/handovers/'+id+'/download',{headers:{Authorization:'Bearer '+session.token}});
    if(!r.ok){const d=await r.json().catch(()=>({detail:'下载失败，请重试'}));throw new Error(d.detail||'下载失败，请重试');}
    const disposition=r.headers.get('content-disposition')||'',match=/filename="?([^";]+)"?/.exec(disposition);
    const name=match?decodeURIComponent(match[1]):'handover-'+id+'.zip';
    const u=URL.createObjectURL(await r.blob()),a=document.createElement('a');a.href=u;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);
  }
};
