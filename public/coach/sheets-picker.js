(() => {
  const button=document.getElementById('open-picker');
  const form=document.getElementById('picker-choice');
  const status=document.getElementById('picker-status');
  button.addEventListener('click',async()=>{
    if (button.disabled) return;
    button.disabled=true;
    status.textContent='Abriendo el selector de Google…';
    try {
      if (!window.gapi?.load) throw Error();
      await new Promise((resolve,reject)=>window.gapi.load('picker',{
        callback:resolve,onerror:reject,timeout:15000,ontimeout:reject
      }));
      const response=await fetch('/api/coach/conexiones/sheets/picker-token',{method:'POST',credentials:'same-origin',cache:'no-store',
        headers:{'Content-Type':'application/json'},body:JSON.stringify({csrf:form.elements.csrf.value,grant:form.elements.grant.value})});
      if (!response.ok) throw Error();
      const {accessToken,pickerKey,appId}=await response.json();
      if (!accessToken || !pickerKey || !appId) throw Error();
      const g=window.google.picker;
      const view=new g.DocsView(g.ViewId.SPREADSHEETS).setMode(g.DocsViewMode.LIST).setMimeTypes('application/vnd.google-apps.spreadsheet');
      const picker=new g.PickerBuilder().addView(view).setOAuthToken(accessToken).setAppId(appId)
        .setDeveloperKey(pickerKey).setOrigin(location.origin).setLocale('es').setCallback(data=>{
          const action=data[g.Response.ACTION];
          if (action===g.Action.PICKED) {
            const file=data[g.Response.DOCUMENTS]?.[0]?.[g.Document.ID];
            if (typeof file==='string' && file) { form.elements.spreadsheetId.value=file; form.submit(); return; }
          }
          if (action===g.Action.CANCEL || action===g.Action.PICKED) {
            button.disabled=false; status.textContent='Puedes elegir otro archivo.';
          }
        }).build();
      picker.setVisible(true);
      status.textContent='';
    } catch {
      status.textContent='No se pudo abrir Google Drive. Vuelve a Mis conexiones y comprueba la cuenta de Google.';
      button.disabled=false;
    }
  });
})();
