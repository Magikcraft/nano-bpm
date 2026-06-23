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
    '.s-active-tag{font-size:10px;color:#86efac;border:1px solid #14532d;border-radius:999px;padding:0 7px}'
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
      '<label>Provider <select id="s-provider">' +
        '<option value="">(env default)</option>' +
        '<option value="openai">openai / local</option>' +
        '<option value="anthropic">anthropic</option>' +
      '</select></label>' +
      '<label>Base URL <input id="s-baseUrl" placeholder="e.g. http://localhost:8888/v1"></label>' +
      '<label>Model' +
        '<div class="row"><input id="s-model" list="s-modellist" placeholder="model id" style="flex:3">' +
        '<button id="s-fetch" class="ghost" style="flex:1" title="Query the endpoint for its models">Fetch</button></div>' +
        '<datalist id="s-modellist"></datalist>' +
      '</label>' +
      '<label>API key <input id="s-apiKey" type="password" placeholder="(unset)"></label>' +
      '<div class="row">' +
        '<label>Max tokens <input id="s-maxTokens" type="number" min="1" placeholder="2048" title="Token budget; Fetch fills this from the model\u2019s context window"></label>' +
        '<label>Temperature <input id="s-temp" type="number" step="0.05" min="0" placeholder="0.2"></label>' +
      '</div>' +
      '<div class="bar"><button id="s-save" class="primary">Save profile</button><button id="s-clearkey" class="ghost">Clear API key</button></div>' +
      '<div class="sep"></div>' +
      '<h4>Python (analysis escape hatch)</h4>' +
      '<label>Interpreter <input id="s-python" placeholder="python3 or /path/to/venv/bin/python"></label>' +
      '<div class="bar"><button id="s-savepy" class="ghost">Save interpreter</button></div>' +
      '<div id="s-status"></div>' +
      '<div id="s-path"></div>' +
    '</div></div></div>';

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

  function fillFields() {
    var p = currentProfile();
    if (!p) {
      ['s-name', 's-baseUrl', 's-model', 's-maxTokens', 's-temp'].forEach(function (i) { $(i).value = ''; });
      $('s-provider').value = ''; $('s-apiKey').value = '';
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
    $('s-path').textContent = v.path ? ('Persisted to ' + v.path) : '';
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
    var patch = {
      name: $('s-name').value,
      provider: $('s-provider').value,
      baseUrl: $('s-baseUrl').value,
      model: $('s-model').value,
      maxTokens: $('s-maxTokens').value ? Number($('s-maxTokens').value) : 0,
      temperature: $('s-temp').value !== '' ? Number($('s-temp').value) : -1
    };
    var key = $('s-apiKey').value;
    if (key) patch.apiKey = key;
    return patch;
  }

  $('cog').addEventListener('click', function () { overlay.hidden = false; reload(); });
  $('s-close').addEventListener('click', function () { overlay.hidden = true; });
  overlay.addEventListener('click', function (e) { if (e.target === overlay) overlay.hidden = true; });
  $('s-profile').addEventListener('change', fillFields);

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
})();
