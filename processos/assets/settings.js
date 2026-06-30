/* ProcessOS settings panel — a self-contained module shared by the console and the
 * cockpit. Injects its own styles, a lower-left gear button, and a slide-in properties
 * panel for managing LLM connection *profiles* (add / edit / delete / switch active /
 * fetch the endpoint's model list) plus the global Python interpreter.
 *
 * Include with: <script src="/assets/settings.js"></script>  (no markup required). */
(function () {
  if (window.__processosSettingsLoaded) return;
  window.__processosSettingsLoaded = true;

  var CSS = [
    '#cog{position:fixed;left:14px;bottom:14px;width:38px;height:38px;border-radius:50%;font-size:18px;line-height:1;display:flex;align-items:center;justify-content:center;background:#18181b;border:1px solid #3f3f46;color:#a1a1aa;cursor:pointer;z-index:50;padding:0}',
    '#cog:hover{color:#e4e4e7;border-color:#6366f1}',
    '#settings-overlay{position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:60;display:flex}',
    '#settings-overlay[hidden]{display:none}',
    '#settings-panel{margin:0 auto 0 0;width:380px;max-width:94vw;height:100%;background:#0f0f11;border-right:1px solid #27272a;box-shadow:0 0 40px rgba(0,0,0,.6);display:flex;flex-direction:column;color:#e4e4e7;font:14px/1.5 system-ui,sans-serif}',
    '#settings-panel .ph{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid #27272a}',
    '#settings-panel .ph button{background:none;border:none;color:#a1a1aa;font-size:16px;cursor:pointer}',
    '.settings-body{padding:16px;display:flex;flex-direction:column;gap:12px;overflow:auto}',
    '.settings-body h4{margin:6px 0 0;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#71717a}',
    '.settings-body label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:#a1a1aa}',
    '.settings-body input,.settings-body select{background:#18181b;border:1px solid #3f3f46;border-radius:6px;color:#e4e4e7;padding:6px 8px;font:inherit}',
    '.settings-body .row{display:flex;gap:8px}',
    '.settings-body .row label,.settings-body .row>*{flex:1}',
    '.settings-body button{background:#27272a;color:#e4e4e7;border:1px solid #3f3f46;border-radius:6px;padding:6px 10px;cursor:pointer;font:inherit}',
    '.settings-body button.primary{background:#312e81;border-color:#4f46e5;color:#e0e7ff}',
    '.settings-body button.ghost{background:none}',
    '.settings-body .bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}',
    '.settings-body .sep{height:1px;background:#1f1f23;margin:6px 0}',
    '#s-status{color:#a1a1aa;font-size:12px;min-height:16px}',
    '#s-path{color:#71717a;font-size:11px;word-break:break-all}',
    '.s-active-tag{font-size:10px;color:#86efac;border:1px solid #14532d;border-radius:999px;padding:0 7px}',
    '.settings-body .hint{font-size:11px;color:#71717a}',
    '.settings-body .hint a{color:#818cf8}',
    '.settings-body label.ck{flex-direction:row;align-items:center;gap:7px;color:#e4e4e7;cursor:pointer}',
    '.settings-body label.ck input{flex:0 0 auto;width:auto}',
    '.settings-body .s-types{display:flex;gap:10px;margin:2px 0 2px}',
    '.settings-body label.rd{flex:1;flex-direction:row;align-items:center;gap:7px;color:#e4e4e7;cursor:pointer;background:#18181b;border:1px solid #3f3f46;border-radius:6px;padding:7px 9px;font-size:13px}',
    '.settings-body label.rd input{flex:0 0 auto;width:auto}',
    '.settings-body label.rd.sel{border-color:#6366f1;background:#1e1b3a}',
    '.settings-body .s-typefields{display:flex;flex-direction:column;gap:12px}',
    '.settings-body .s-typefields[hidden]{display:none}',
    '#s-sidecar-controls[hidden]{display:none}',
    '#s-llama-status{font-size:12px;color:#a1a1aa;min-height:16px}',
    '.s-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#52525b;margin-right:6px;vertical-align:middle}',
    '.s-dot.on{background:#22c55e}',
    '#llama-logs-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:70;display:flex}',
    '#llama-logs-overlay[hidden]{display:none}',
    '#llama-logs-panel{margin:auto;width:780px;max-width:94vw;height:80vh;background:#0b0b0d;border:1px solid #27272a;border-radius:8px;display:flex;flex-direction:column;color:#e4e4e7;font:13px/1.5 system-ui,sans-serif}',
    '#llama-logs-panel .llh{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;border-bottom:1px solid #27272a}',
    '#llama-logs-panel .llh strong{font-size:13px}',
    '#llama-logs-panel .llh button{background:none;border:none;color:#a1a1aa;font-size:16px;cursor:pointer}',
    '.ll-cmd{padding:8px 14px;font:12px/1.45 ui-monospace,Menlo,monospace;color:#a5b4fc;border-bottom:1px solid #1f1f23;word-break:break-all;background:#111}',
    '.ll-cmd .lbl{display:block;color:#71717a;font-family:system-ui,sans-serif;font-size:10px;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px}',
    '#ll-body{margin:0;padding:10px 14px;overflow:auto;flex:1;font:12px/1.45 ui-monospace,Menlo,monospace;color:#d4d4d8;white-space:pre-wrap;word-break:break-word}'
  ].join('\n');

  var HTML =
    '<button id="cog" title="Settings" aria-label="Settings">&#9881;</button>' +
    '<div id="settings-overlay" hidden><div id="settings-panel" role="dialog" aria-label="Settings">' +
    '<div class="ph"><strong>Settings</strong><button id="s-close" aria-label="Close">&#10005;</button></div>' +
    '<div class="settings-body">' +
      '<h4>LLM profiles</h4>' +
      '<div class="bar">' +
        '<select id="s-profile" style="flex:1"></select>' +
        '<button id="s-add" class="ghost" title="Add profile">+ Add</button>' +
        '<button id="s-del" class="ghost" title="Delete profile">Delete</button>' +
      '</div>' +
      '<div class="bar"><span id="s-activeline"></span><button id="s-setactive" class="ghost">Set active</button></div>' +
      '<div class="sep"></div>' +
      '<label>Name <input id="s-name" placeholder="e.g. Local (llama.cpp)"></label>' +
      '<div class="s-types">' +
        '<label class="rd" id="s-type-sidecar-l"><input type="radio" name="s-type" id="s-type-sidecar" value="sidecar"> Managed sidecar</label>' +
        '<label class="rd" id="s-type-external-l"><input type="radio" name="s-type" id="s-type-external" value="external"> External</label>' +
      '</div>' +
      '<div id="s-fields-sidecar" class="s-typefields">' +
        '<div class="hint">ProcessOS launches <code>llama-server</code> for this model and talks to it locally.</div>' +
        '<label>Model file / HF spec <input id="s-modelFile" placeholder="unsloth/Qwen3-4B-GGUF:UD-Q4_K_XL or /path/model.gguf"></label>' +
        '<div class="hint">ProcessOS assigns a free local port automatically when you start the sidecar — no port to configure.</div>' +
        '<label>Startup args <input id="s-sidecarArgs" placeholder="-ngl 99 -c 32768 --jinja"></label>' +
        '<div class="hint">A <code>:quant</code> HF spec is downloaded into the models directory; a <code>.gguf</code> path is resolved against it. Browse GGUF models on <a href="https://huggingface.co/models?library=gguf&sort=trending" target="_blank" rel="noopener noreferrer">HuggingFace</a>.</div>' +
      '</div>' +
      '<div id="s-fields-external" class="s-typefields">' +
        '<label>Provider <select id="s-provider">' +
          '<option value="">(env default)</option>' +
          '<option value="openai">openai / local</option>' +
          '<option value="anthropic">anthropic</option>' +
        '</select></label>' +
        '<label>Base URL <input id="s-baseUrl" placeholder="e.g. https://api.openai.com/v1"></label>' +
        '<label>Model' +
          '<div class="row"><input id="s-model" list="s-modellist" placeholder="model id" style="flex:3">' +
          '<button id="s-fetch" class="ghost" style="flex:1" title="Query the endpoint for its models">Fetch</button></div>' +
          '<datalist id="s-modellist"></datalist>' +
        '</label>' +
        '<label>API key <input id="s-apiKey" type="password" placeholder="(unset)"></label>' +
      '</div>' +
      '<div class="row">' +
        '<label>Max tokens <input id="s-maxTokens" type="number" min="1" placeholder="2048" title="Token budget; Fetch fills this from the model\u2019s context window"></label>' +
        '<label>Temperature <input id="s-temp" type="number" step="0.05" min="0" placeholder="0.2"></label>' +
        '<label title="Caps reasoning tokens as a fraction of Max tokens (Fast 20%, Medium 50%, Max 100%). Sent as thinking_budget_tokens/reasoning_budget; ignored by models without reasoning control.">Thinking level ' +
          '<select id="s-thinking">' +
            '<option value="">Default (unconstrained)</option>' +
            '<option value="fast">Fast (20%)</option>' +
            '<option value="medium">Medium (50%)</option>' +
            '<option value="max">Max (100%)</option>' +
          '</select>' +
        '</label>' +
      '</div>' +
      '<div class="bar"><button id="s-save" class="primary">Save profile</button><button id="s-clearkey" class="ghost">Clear API key</button></div>' +
      '<div class="sep"></div>' +
      '<div id="s-sidecar-controls">' +
        '<div class="bar"><button id="s-llama-start" class="ghost">Start sidecar</button><button id="s-llama-stop" class="ghost">Stop</button><button id="s-llama-logs" class="ghost">Logs</button></div>' +
        '<div id="s-llama-status"></div>' +
        '<div class="sep"></div>' +
      '</div>' +
      '<h4>Local model server (llama.cpp)</h4>' +
      '<label>Models directory <input id="s-modelsDir" placeholder="(default)"></label>' +
      '<label>llama-server binary <input id="s-llamaBin" placeholder="llama-server (found on PATH)"></label>' +
      '<div class="bar"><button id="s-savellama" class="ghost">Save server config</button></div>' +
      '<div class="sep"></div>' +
      '<h4>Python (analysis escape hatch)</h4>' +
      '<label>Interpreter <input id="s-python" placeholder="python3 or /path/to/venv/bin/python"></label>' +
      '<div class="bar"><button id="s-savepy" class="ghost">Save interpreter</button></div>' +
      '<div id="s-status"></div>' +
      '<div id="s-path"></div>' +
    '</div></div></div>' +
    '<div id="llama-logs-overlay" hidden><div id="llama-logs-panel" role="dialog" aria-label="llama-server output">' +
      '<div class="llh"><strong>llama-server output</strong><button id="ll-close" aria-label="Close">&#10005;</button></div>' +
      '<div class="ll-cmd"><span class="lbl">Run it yourself in a terminal</span><span id="ll-cmd"></span></div>' +
      '<pre id="ll-body"></pre>' +
    '</div></div>';

  var style = document.createElement('style');
  style.textContent = CSS;
  document.head.appendChild(style);
  var holder = document.createElement('div');
  holder.innerHTML = HTML;
  while (holder.firstChild) document.body.appendChild(holder.firstChild);

  var $ = function (id) { return document.getElementById(id); };
  var overlay = $('settings-overlay');
  var STATE = { view: null };

  function keyPh(set) { return set ? '\u2022\u2022\u2022\u2022 set \u2014 leave blank to keep' : '(unset)'; }
  function status(msg) { $('s-status').textContent = msg || ''; }

  // Context windows learned from the last endpoint fetch, keyed by model id.
  var MODEL_CTX = {};
  // Fill the Max tokens field from a model's fetched context window when we know it, so
  // picking a model sizes the token budget to the endpoint's reported window.
  function applyModelContext(modelId) {
    var ctx = modelId && MODEL_CTX[modelId];
    if (ctx) { $('s-maxTokens').value = ctx; }
  }

  function currentProfile() {
    if (!STATE.view) return null;
    var id = $('s-profile').value;
    return STATE.view.profiles.find(function (p) { return p.id === id; }) || null;
  }

  function renderProfileList() {
    var v = STATE.view, sel = $('s-profile'), active = v.activeProfile;
    var keep = sel.value;
    sel.innerHTML = '';
    v.profiles.forEach(function (p) {
      var o = document.createElement('option');
      o.value = p.id;
      o.textContent = p.name + (p.id === active ? '  \u2713 active' : '');
      sel.appendChild(o);
    });
    if (v.profiles.some(function (p) { return p.id === keep; })) sel.value = keep;
    else if (active) sel.value = active;
  }

  function currentType() {
    return $('s-type-sidecar').checked ? 'sidecar' : 'external';
  }

  // Show only the fields for the selected profile type; the sidecar runtime controls
  // (Start/Stop/Logs) and the highlighted radio follow suit.
  function applyTypeUI() {
    var t = currentType();
    $('s-fields-sidecar').hidden = t !== 'sidecar';
    $('s-fields-external').hidden = t !== 'external';
    $('s-sidecar-controls').hidden = t !== 'sidecar';
    $('s-type-sidecar-l').classList.toggle('sel', t === 'sidecar');
    $('s-type-external-l').classList.toggle('sel', t === 'external');
  }

  function fillFields() {
    var p = currentProfile();
    if (!p) {
      ['s-name', 's-baseUrl', 's-model', 's-maxTokens', 's-temp', 's-modelFile', 's-sidecarArgs'].forEach(function (i) { $(i).value = ''; });
      $('s-provider').value = ''; $('s-apiKey').value = '';
      $('s-thinking').value = '';
      $('s-type-external').checked = true;
      applyTypeUI();
      $('s-activeline').textContent = 'No profiles';
      return;
    }
    $('s-name').value = p.name || '';
    $('s-provider').value = p.provider || '';
    $('s-baseUrl').value = p.baseUrl || '';
    $('s-model').value = p.model || '';
    $('s-apiKey').value = ''; $('s-apiKey').placeholder = keyPh(p.apiKeySet);
    $('s-maxTokens').value = p.maxTokens != null ? p.maxTokens : '';
    $('s-temp').value = p.temperature != null ? p.temperature : '';
    $('s-modelFile').value = p.modelFile || '';
    $('s-sidecarArgs').value = p.sidecarArgs || '';
    $('s-thinking').value = p.thinkingLevel || '';
    if (p.sidecar) $('s-type-sidecar').checked = true; else $('s-type-external').checked = true;
    applyTypeUI();
    var isActive = p.id === STATE.view.activeProfile;
    $('s-activeline').innerHTML = isActive
      ? 'This profile is <span class="s-active-tag">active</span>'
      : 'Not the active profile';
    $('s-modellist').innerHTML = '';
  }

  function applyView(v) {
    STATE.view = v;
    renderProfileList();
    fillFields();
    $('s-python').value = v.pythonBin || '';
    $('s-modelsDir').value = v.modelsDir || '';
    $('s-modelsDir').placeholder = v.defaultModelsDir || '(default)';
    $('s-llamaBin').value = v.llamaBin || '';
    $('s-path').textContent = v.path ? ('Persisted to ' + v.path) : '';
    refreshLlama();
    // Let host surfaces (e.g. the cockpit's Send button + Python label) react to a change.
    try { window.dispatchEvent(new CustomEvent('processos:settings-changed', { detail: v })); }
    catch (e) { /* CustomEvent unsupported — non-fatal */ }
  }

  async function api(method, url, body) {
    var opt = { method: method, headers: { 'content-type': 'application/json' } };
    if (body !== undefined) opt.body = JSON.stringify(body);
    var r = await fetch(url, opt);
    var d = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(d.error || (method + ' ' + url + ' -> ' + r.status));
    return d;
  }

  async function reload() {
    try { applyView(await api('GET', '/api/settings')); status(''); }
    catch (e) { status('load failed: ' + e.message); }
  }

  function profilePatch() {
    var type = currentType();
    var patch = {
      name: $('s-name').value,
      maxTokens: $('s-maxTokens').value ? Number($('s-maxTokens').value) : 0,
      temperature: $('s-temp').value !== '' ? Number($('s-temp').value) : -1,
      thinkingLevel: $('s-thinking').value || '',
      sidecar: type === 'sidecar'
    };
    if (type === 'sidecar') {
      var modelFile = $('s-modelFile').value;
      patch.modelFile = modelFile;
      patch.sidecarArgs = $('s-sidecarArgs').value;
      patch.provider = 'openai';
      // Port/base URL are managed by ProcessOS (assigned on start); leave baseUrl untouched so the
      // running endpoint is preserved across edits.
      patch.model = modelFile; // llama-server serves under the loaded model name
    } else {
      patch.provider = $('s-provider').value;
      patch.baseUrl = $('s-baseUrl').value;
      patch.model = $('s-model').value;
      patch.modelFile = ''; // not a sidecar — clear any managed-model fields
      patch.sidecarArgs = '';
      var ctx = MODEL_CTX[$('s-model').value];
      if (ctx) patch.contextWindow = ctx; // persist the model's advertised context window
      var key = $('s-apiKey').value;
      if (key) patch.apiKey = key;
    }
    return patch;
  }

  $('cog').addEventListener('click', function () { overlay.hidden = false; reload(); });
  $('s-close').addEventListener('click', function () { overlay.hidden = true; });
  overlay.addEventListener('click', function (e) { if (e.target === overlay) overlay.hidden = true; });
  $('s-profile').addEventListener('change', fillFields);
  $('s-type-sidecar').addEventListener('change', applyTypeUI);
  $('s-type-external').addEventListener('change', applyTypeUI);

  // Open the panel from another surface (e.g. the cockpit's LLM sidecars view),
  // optionally focused on a specific profile.
  window.processosOpenSettings = function (profileId) {
    overlay.hidden = false;
    reload().then(function () {
      if (profileId && STATE.view && STATE.view.profiles.some(function (p) { return p.id === profileId; })) {
        $('s-profile').value = profileId;
        fillFields();
      }
    });
  };

  $('s-add').addEventListener('click', async function () {
    var name = prompt('New profile name:', 'New profile');
    if (name == null) return;
    try {
      var d = await api('POST', '/api/settings/profiles', { name: name });
      applyView(d.settings);
      $('s-profile').value = d.id; fillFields();
      status('Profile added \u2014 edit and Save');
    } catch (e) { status('error: ' + e.message); }
  });

  $('s-del').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) return;
    if (!confirm('Delete profile "' + p.name + '"?')) return;
    try { applyView(await api('DELETE', '/api/settings/profiles/' + encodeURIComponent(p.id))); status('Profile deleted'); }
    catch (e) { status('error: ' + e.message); }
  });

  $('s-setactive').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) return;
    try { applyView(await api('PUT', '/api/settings', { activeProfile: p.id })); status('Active profile set to "' + p.name + '"'); }
    catch (e) { status('error: ' + e.message); }
  });

  $('s-save').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) { status('add a profile first'); return; }
    status('Saving\u2026');
    try { applyView(await api('PUT', '/api/settings/profiles/' + encodeURIComponent(p.id), profilePatch())); status('Saved \u2713'); }
    catch (e) { status('error: ' + e.message); }
  });

  $('s-clearkey').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) return;
    try { applyView(await api('PUT', '/api/settings/profiles/' + encodeURIComponent(p.id), { apiKey: '' })); status('API key cleared'); }
    catch (e) { status('error: ' + e.message); }
  });

  $('s-fetch').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) { status('add a profile first'); return; }
    status('Fetching models\u2026');
    var body = { profileId: p.id, provider: $('s-provider').value, baseUrl: $('s-baseUrl').value };
    if ($('s-apiKey').value) body.apiKey = $('s-apiKey').value;
    try {
      var d = await api('POST', '/api/settings/models', body);
      var dl = $('s-modellist'); dl.innerHTML = '';
      MODEL_CTX = {};
      d.models.forEach(function (m) {
        var id = typeof m === 'string' ? m : m.id;
        var ctx = (m && typeof m === 'object') ? m.contextWindow : null;
        if (ctx) MODEL_CTX[id] = ctx;
        var o = document.createElement('option');
        o.value = id;
        if (ctx) o.label = id + ' (' + ctx.toLocaleString() + ' ctx)';
        dl.appendChild(o);
      });
      if (!$('s-model').value && d.models.length === 1) {
        $('s-model').value = typeof d.models[0] === 'string' ? d.models[0] : d.models[0].id;
      }
      applyModelContext($('s-model').value);
      var ctxNote = '';
      var sel = $('s-model').value;
      if (sel && MODEL_CTX[sel]) ctxNote = ' \u2014 context window ' + MODEL_CTX[sel].toLocaleString() + ' tokens';
      status('Found ' + d.models.length + ' model(s) \u2014 pick one from the field' + ctxNote);
      $('s-model').focus();
    } catch (e) { status('fetch failed: ' + e.message); }
  });

  // When the operator picks/edits a model, fill Max tokens from its fetched context window.
  $('s-model').addEventListener('change', function () { applyModelContext($('s-model').value); });

  $('s-savepy').addEventListener('click', async function () {
    status('Saving\u2026');
    try { applyView(await api('PUT', '/api/settings', { pythonBin: $('s-python').value })); status('Interpreter saved'); }
    catch (e) { status('error: ' + e.message); }
  });

  // --- Local llama.cpp sidecar ---------------------------------------------
  var LLAMA = { command: '', logTimer: null, since: 0 };

  // Find the running sidecar entry for the profile shown in the editor, if any.
  function currentSidecar(list) {
    var p = currentProfile();
    if (!p || !list || !list.sidecars) return null;
    return list.sidecars.find(function (s) { return s.profileId === p.id; }) || null;
  }

  function llamaStatusLine(s) {
    var el = $('s-llama-status');
    if (!el) return;
    if (s && s.running) {
      el.innerHTML = '<span class="s-dot on"></span>Running ' + (s.model || '') +
        ' on port ' + (s.port != null ? s.port : '?') + ' (pid ' + (s.pid != null ? s.pid : '?') + ')';
    } else if (s && s.error) {
      el.innerHTML = '<span class="s-dot"></span>Exited: ' + s.error;
    } else {
      el.innerHTML = '<span class="s-dot"></span>This profile\u2019s sidecar is not running';
    }
  }

  async function refreshLlama() {
    if (!$('s-llama-status')) return;
    try {
      var list = await api('GET', '/api/llama/status');
      var s = currentSidecar(list);
      LLAMA.command = (s && s.command) || '';
      llamaStatusLine(s);
    } catch (e) { /* status endpoint absent on older servers — non-fatal */ }
  }

  $('s-savellama').addEventListener('click', async function () {
    status('Saving\u2026');
    try {
      applyView(await api('PUT', '/api/settings', { modelsDir: $('s-modelsDir').value, llamaBin: $('s-llamaBin').value }));
      status('Server config saved');
    } catch (e) { status('error: ' + e.message); }
  });

  $('s-llama-start').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) { status('add a sidecar profile first'); return; }
    status('Starting sidecar\u2026');
    try {
      var s = await api('POST', '/api/llama/start', { profileId: p.id });
      LLAMA.command = s.command || ''; llamaStatusLine(s);
      status(s.running ? 'Sidecar started \u2014 open Logs to watch it load' : 'Sidecar did not start');
    } catch (e) { status('start failed: ' + e.message); llamaStatusLine(null); }
  });

  $('s-llama-stop').addEventListener('click', async function () {
    var p = currentProfile(); if (!p) return;
    status('Stopping sidecar\u2026');
    try { await api('POST', '/api/llama/stop', { profileId: p.id }); await refreshLlama(); status('Sidecar stopped'); }
    catch (e) { status('stop failed: ' + e.message); }
  });

  var logsOverlay = $('llama-logs-overlay');
  function stopLogPolling() { if (LLAMA.logTimer) { clearInterval(LLAMA.logTimer); LLAMA.logTimer = null; } }
  function closeLogs() { logsOverlay.hidden = true; stopLogPolling(); }

  async function pollLogs() {
    var p = currentProfile(); if (!p) return;
    try {
      var d = await api('GET', '/api/llama/logs?profileId=' + encodeURIComponent(p.id) + '&since=' + LLAMA.since);
      if (d.lines && d.lines.length) {
        var body = $('ll-body');
        var atBottom = body.scrollTop + body.clientHeight >= body.scrollHeight - 24;
        body.textContent += d.lines.join('\n') + '\n';
        if (atBottom) body.scrollTop = body.scrollHeight;
      }
      if (typeof d.nextOffset === 'number') LLAMA.since = d.nextOffset;
    } catch (e) { /* keep polling; transient errors are non-fatal */ }
  }

  $('s-llama-logs').addEventListener('click', async function () {
    LLAMA.since = 0;
    $('ll-body').textContent = '';
    await refreshLlama();
    $('ll-cmd').textContent = LLAMA.command || '(start the sidecar to see its command)';
    logsOverlay.hidden = false;
    await pollLogs();
    stopLogPolling();
    LLAMA.logTimer = setInterval(pollLogs, 1500);
  });

  $('ll-close').addEventListener('click', closeLogs);
  logsOverlay.addEventListener('click', function (e) { if (e.target === logsOverlay) closeLogs(); });
})();
