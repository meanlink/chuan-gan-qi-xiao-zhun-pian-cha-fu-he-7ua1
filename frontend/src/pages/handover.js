import {handovers,datasets} from '../api/entities.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {esc,date,notify} from '../utils/dom.js';
import {HandoverStatus} from '../constants/enums.js';

const STATUS=HandoverStatus;
const STATUS_HINT={
  validating:'正在校验包内引用与文件指纹，暂未写入任何业务数据。',
  importing:'归档已写入，正在以单事务落库；整组记录在此阶段不可见。',
  recovering:'上次导入被中断，暂存文件已保留。重新导入同一交接包即可继续，不会重复迁入。',
  failed:'整包已拒绝，未写入任何样本、分析、计划或归档。',
  completed:'整组已迁入，样本、分析、计划与归档可正常查看。'
};

function mapping(h){
  const map=h.id_map||{},pairs=[];
  for(const [kind,label] of [['dataset','样本'],['jobs','分析'],['reports','计划'],['deliveries','归档']]){
    const entries=Object.entries(map[kind]||{});
    if(!entries.length)continue;
    pairs.push(label+' '+entries.slice(0,3).map(([from,to])=>'#'+esc(from)+' → #'+esc(to)).join('，')+(entries.length>3?' 等 '+entries.length+' 项':''));
  }
  return pairs.length?'<p class="handover-map">原编号 → 迁入编号：'+pairs.join('；')+'</p>':'';
}

export async function handoverPage(){
  const [records,list]=await Promise.all([handovers.list(),datasets.list()]);
  workspace.handovers=records;workspace.datasets=list;
  const canWrite=session.user.role!=='viewer';
  const exports=records.filter(h=>h.direction==='export');
  const imports=records.filter(h=>h.direction==='import');
  const badge=h=>'<span class="status handover-'+esc(h.status)+'">'+esc(STATUS[h.status]||h.status)+'</span>';

  const exportRows=exports.map(h=>'<article class="handover-row"><div><strong>'+esc(h.filename)+'</strong><p>'+esc(h.summary?.dataset_name||'')+' · 样本 '+esc(h.summary?.dataset??0)+' · 分析 '+esc(h.summary?.jobs??0)+' · 计划 '+esc(h.summary?.reports??0)+' · 归档 '+esc(h.summary?.deliveries??0)+'</p><small>'+date(h.created_at)+' · SHA256 '+esc(String(h.fingerprint).slice(0,24))+'…</small></div><div class="handover-actions">'+badge(h)+'<button class="text-button" data-package-download="'+h.id+'">下载 zip ↓</button></div></article>').join('');

  const importRows=imports.map(h=>'<article class="handover-row handover-'+esc(h.status)+'"><div><strong>'+esc(h.filename)+'</strong><p>'+esc(STATUS[h.status]||h.status)+' · '+(h.status==='completed'?(h.summary?.renamed?'目标编号已重新分配，原有数据未覆盖':'按原编号迁入'):esc(STATUS_HINT[h.status]||''))+'</p><small>'+date(h.updated_at)+' · '+esc(h.package_id||'')+'</small>'+mapping(h)+(h.errors?.length?'<details class="handover-errors" open><summary>校验明细（'+h.errors.length+' 项）</summary><ul>'+h.errors.map(e=>'<li>'+esc(e)+'</li>').join('')+'</ul></details>':'')+(h.status==='completed'&&h.dataset_id?'<a class="text-link" href="#/datasets">查看迁入批次 ↗</a>':'')+'</div><div class="handover-actions">'+badge(h)+'</div></article>').join('');

  return heading('06 / HANDOVER','批次交接包','按批次导出样本、已完成分析、简报计划与归档；接收方保留现有数据，迁入记录保留原始编号关系。')+'<div class="handover-grid">'+
    '<section class="panel"><div class="section-title"><h2>导出交接包</h2><span>ZIP · 含指纹清单</span></div><form id="handover-export"><label>数据批次<select name="dataset_id" required>'+list.map(d=>'<option value="'+d.id+'">'+esc(d.name)+' · '+d.rows+' 条</option>').join('')+'</select></label><button type="submit" class="primary" '+(canWrite&&list.length?'':'disabled')+'>生成交接包 ↗</button><p class="hint">包含该批次全部已完成分析、由这些分析创建的简报计划，以及对应归档原件。归档缺失或校验不符时整包拒绝导出。</p></form>'+
    '<div class="section-title"><h2>已导出</h2><span>'+exports.length+' 个</span></div><div class="handover-list">'+(exportRows||'<p class="empty">尚未导出交接包。</p>')+'</div></section>'+
    '<section class="panel"><div class="section-title"><h2>导入交接包</h2><span>整包校验后落库</span></div><form id="handover-import"><label>选择交接包 ZIP<input type="file" id="handover-file" accept=".zip,application/zip"></label><button type="submit" class="primary" '+(canWrite?'':'disabled')+'>导入并校验 ↗</button><p class="hint">包内引用缺失或文件校验失败时整包拒绝，不写入任何记录；同一交接包重复或并发导入只迁入一组。迁入计划默认停用。</p></form>'+
    '<div class="section-title"><h2>迁入记录</h2><span>'+imports.length+' 条</span></div><div class="handover-list">'+(importRows||'<p class="empty">尚未导入交接包。</p>')+'</div></section></div>';
}

export function bindHandover(refresh){
  const exportForm=document.querySelector('#handover-export');
  exportForm?.addEventListener('submit',async e=>{
    e.preventDefault();const button=exportForm.querySelector('[type=submit]');button.disabled=true;
    try{
      const dataset_id=Number(new FormData(exportForm).get('dataset_id'));
      const h=await handovers.export(dataset_id);await refresh();
      notify('交接包已生成：'+(h?.filename||''));
    }catch(error){notify(error.message,true);}finally{button.disabled=false;}
  });
  const importForm=document.querySelector('#handover-import');
  importForm?.addEventListener('submit',async e=>{
    e.preventDefault();const button=importForm.querySelector('[type=submit]');button.disabled=true;
    const input=document.querySelector('#handover-file'),file=input?.files?.[0];
    try{
      if(!file)throw new Error('请选择交接包 ZIP 文件');
      const h=await handovers.import(file);
      if(h.status==='completed')notify('交接包已导入完成');
      else if(h.status==='recovering')notify('导入被中断，已保留暂存文件；请重新导入同一交接包继续',true);
      else notify('交接包已拒绝：'+(h.errors?.[0]||'请查看校验明细'),true);
      await refresh();
    }catch(error){notify(error.message,true);}finally{button.disabled=false;}
  });
  document.querySelectorAll('[data-package-download]').forEach(button=>button.onclick=async()=>{
    try{await handovers.download(Number(button.dataset.packageDownload));}catch(error){notify(error.message,true);}
  });
}
