# VC Manager for Nighty
# Authors: @rico, @snowy, @factz
# Voice controls, session data, and multi-token account management for Nighty

@nightyScript(
    name="VC Manager v4.3.0",
    author="@rico, @snowy, @factz",
    description="Manage voice connections, controls, reconnects, follow mode, timers, and session stats.",
    usage="/vc <join|load|leave|mute|deafen|stream|camera|list|history|status|stats|settings|help>"
)
def vc_manager_farm_script():
    """
    VC MANAGER
    ----------
    Discord voice-channel management, telemetry, automation, and optional
    secondary-account gateway connections for Nighty.

    SLASH COMMANDS:
    /vc join <channel>      - Join a voice channel by ID or name
    /vc load <channel_id>   - Join and enable mute, deafen, stream, and camera
    /vc leave               - Leave the managed voice channel
    /vc mute <state>        - Explicitly enable or disable microphone mute
    /vc deafen <state>      - Explicitly enable or disable audio deafen
    /vc stream <state>      - Explicitly enable or disable screen share
    /vc camera <state>      - Explicitly enable or disable camera video
    /vc list                - List current participants
    /vc history             - Show recent session history
    /vc status              - Show the managed connection state
    /vc stats               - Show tracked voice usage statistics
    /vc settings            - View or update slash-response settings
    /vc help                - Show the command overview

    DEPENDENCIES:
    - aiohttp (normally bundled with Nighty; install with: pip install aiohttp)
    - Slash registration is adapted from lord-m1nx/nighty-slash-api and is
      included directly in this script; no additional package is required.

    NOTES:
    - Slash responses use Discord Components V2 and the active Nighty V2 theme.
      A plain-text response is used only when Discord rejects a V2 response.
    - Slash controls are restricted to the Nighty account owner.
    - VC Manager tracks only voice sessions opened through this script. Joining
      a call normally in Discord or Nighty does not change the manager panel.
    - One Discord account can have only one voice state per server. Use a
      secondary managed account when two simultaneous calls are in one server.
    - Secondary Discord user tokens are stored locally in the Nighty scripts
      JSON directory. Keep that directory private.
    - Each managed account can save its own server and channel. Recoverable
      gateway drops restore that destination until Leave is pressed.
    - Managed-account session logs are stored per account. Bulk controls can
      join saved destinations and apply one voice state to all connected accounts.
    - The script checks the public project repository for a newer version and
      shows an in-app notice when an update is available.
    - The CUI compatibility layer is initialized inside this function as
      required by the upstream CUI project.
    """

    # Keep imports inside the Nighty entry point. Nighty gives scripts separate
    # global and local namespaces, which can hide module imports from callbacks.
    import asyncio
    import functools
    import hashlib
    import inspect
    import json
    import math
    import os
    import random
    import re
    import shutil
    import threading
    import time
    import uuid
    from datetime import datetime
    from html import escape as html_escape
    from pathlib import Path
    from typing import Literal
    from urllib.parse import quote

    import aiohttp

    SCRIPT_VERSION = "4.3.0"
    PROJECT_URL = "https://github.com/aboveproof/Nighty-VC-Manager"
    VERSION_SOURCE_URL = (
        "https://raw.githubusercontent.com/aboveproof/Nighty-VC-Manager/main/VCManager.py"
    )

    # Nighty replaces scripts in the same Python process. Cancel tasks from the
    # previous VC Manager load so old timers and gateway loops stop updating it.
    runtime_key = "_vc_manager_runtime"
    previous_runtime = getattr(bot, runtime_key, None)
    previous_token_clients = []
    previous_token_states = {}
    previous_managed_target = None
    previous_connection_start_time = None
    if isinstance(previous_runtime, dict):
        previous_clients_by_id = previous_runtime.get("token_clients", {})
        if isinstance(previous_clients_by_id, dict):
            previous_token_clients = list(previous_clients_by_id.values())
            for previous_id, previous_client in previous_clients_by_id.items():
                previous_channel_id = getattr(previous_client, "desired_channel_id", None) or getattr(previous_client, "current_channel_id", None)
                previous_guild_id = getattr(previous_client, "desired_guild_id", None) or getattr(previous_client, "current_guild_id", None)
                if previous_channel_id and previous_guild_id:
                    previous_token_states[str(previous_id)] = {
                        "guild_id": str(previous_guild_id),
                        "channel_id": str(previous_channel_id),
                        "muted": bool(getattr(previous_client, "is_muted", False)),
                        "deafened": bool(getattr(previous_client, "is_deafened", False)),
                        "streaming": bool(getattr(previous_client, "is_streaming", False)),
                        "camera_on": bool(getattr(previous_client, "is_camera_on", False)),
                        "session_started_at": getattr(previous_client, "voice_session_started_at", None),
                        "session_guild_id": getattr(previous_client, "voice_session_guild_id", None),
                        "session_channel_id": getattr(previous_client, "voice_session_channel_id", None),
                    }
        saved_target = previous_runtime.get("managed_target")
        if isinstance(saved_target, dict):
            previous_managed_target = dict(saved_target)
        saved_start_time = previous_runtime.get("connection_start_time")
        if isinstance(saved_start_time, datetime):
            previous_connection_start_time = saved_start_time
        for event_name, previous_listener in previous_runtime.get("listeners", ()):
            try:
                bot.remove_listener(previous_listener, event_name)
            except Exception as e:
                print(f"[VC Manager] Could not remove a stale {event_name} listener: {e}", type_="ERROR")
        for previous_task in list(previous_runtime.get("tasks", ())):
            try:
                if previous_task and not previous_task.done():
                    previous_loop = previous_task.get_loop() if hasattr(previous_task, "get_loop") else None
                    if previous_loop and previous_loop.is_running():
                        previous_loop.call_soon_threadsafe(previous_task.cancel)
                    else:
                        previous_task.cancel()
            except Exception as e:
                print(f"[VC Manager] Could not stop a stale task: {e}", type_="ERROR")

    # Remove listeners left by builds that predate runtime listener tracking.
    # discord.py stores @bot.listen handlers in extra_events.
    legacy_listener_names = {
        "on_voice_state_update": "handle_voice_state_update",
        "on_ready": "handle_bot_ready",
    }
    extra_events = getattr(bot, "extra_events", {})
    if isinstance(extra_events, dict):
        for event_name, function_name in legacy_listener_names.items():
            for old_listener in list(extra_events.get(event_name, ())):
                listener_qualname = getattr(old_listener, "__qualname__", "")
                if (
                    getattr(old_listener, "__name__", "") == function_name
                    and "vc_manager_farm_script.<locals>" in listener_qualname
                ):
                    try:
                        bot.remove_listener(old_listener, event_name)
                    except Exception as e:
                        print(f"[VC Manager] Could not remove legacy {event_name} listener: {e}", type_="ERROR")

    runtime_state = {
        "tasks": set(),
        "token_clients": {},
        "managed_target": previous_managed_target,
        "connection_start_time": previous_connection_start_time,
        "listeners": [],
        "backed_up_files": set(),
        "generation": uuid.uuid4().hex
    }
    setattr(bot, runtime_key, runtime_state)

    # Older releases did not track runtime tasks. Capture their tasks before
    # this load creates new work, then queue cancellation on the bot loop.
    legacy_task_names = {
        "live_timer_ticker", "startup_sync", "timer", "reconnect_later",
        "_gateway_loop", "_heartbeat"
    }
    try:
        bot_loop = getattr(bot, "loop", None)
    except Exception as e:
        bot_loop = None
        print(f"[VC Manager] Bot loop is not ready during cleanup: {e}", type_="ERROR")
    if bot_loop:
        try:
            legacy_tasks = list(asyncio.all_tasks(bot_loop))
        except Exception:
            legacy_tasks = []
        for legacy_task in legacy_tasks:
            try:
                coroutine = legacy_task.get_coro()
                qualname = getattr(coroutine, "__qualname__", "")
                task_name = qualname.rsplit(".", 1)[-1]
                if "vc_manager_farm_script.<locals>" in qualname and task_name in legacy_task_names:
                    if bot_loop.is_running():
                        bot_loop.call_soon_threadsafe(legacy_task.cancel)
                    else:
                        legacy_task.cancel()
            except Exception as e:
                print(f"[VC Manager] Could not stop a legacy task: {e}", type_="ERROR")

    # ==================== CUI INTEGRATION CORE ====================

    class CUI:
        registry = {}
        instances = {}
        state = {}

    def cui_new(type, name=None):
        def decorator(cls):
            e_name = name or cls.__name__
            init_params = getattr(cls, "_init_params", None)
            if init_params is None:
                try:
                    init_params = list(inspect.signature(cls.__init__).parameters.keys())
                except Exception:
                    init_params = []

            CUI.registry[e_name] = {
                "class": cls,
                "type": type,
                "init_params": init_params
            }

            if type == "callable":
                def c_method(self, **kwargs):
                    ins_id = f"__CUI_CALLABLE__{e_name}__{id(self)}_{id(kwargs)}".replace("-", "_")
                    CUI.instances[ins_id] = {"kwargs": kwargs, "type": "callable"}
                    valid_params = CUI.registry[e_name]["init_params"]
                    filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params}
                    cb_names = [k for k, v in filtered_kwargs.items() if callable(v)]
                    props = {k: v for k, v in filtered_kwargs.items() if not callable(v)}
                    props['hook_id'] = ins_id

                    t_ins_kwargs = {k: None for k in valid_params if k != 'self'}
                    t_ins = cls(**t_ins_kwargs)
                    html_template = t_ins.html
                    create_js = t_ins.create_js() if hasattr(t_ins, 'create_js') and callable(t_ins.create_js) else None
                    remove_js = t_ins.remove_js() if hasattr(t_ins, 'remove_js') and callable(t_ins.remove_js) else None
                    allow_outside_click = kwargs.get('allow_outside_click', True)

                    payload = {
                        "hookId": ins_id,
                        "html": html_template,
                        "props": props,
                        "callbacks": cb_names,
                        "create_js": create_js,
                        "remove_js": remove_js,
                        "allowOutsideClick": allow_outside_click
                    }
                    p_json = json.dumps(payload)
                    evaluate_js(f"window.cUIData.renderCallable({json.dumps(p_json)})")

                setattr(Tab, e_name.lower(), c_method)
                return cls

            elif type == "element":
                class CUIElementAtts:
                    def __init__(self, **kwargs):
                        self._hook_id = f"__CUI__{e_name}__{id(self)}"
                        CUI.instances[self._hook_id] = {"kwargs": kwargs, "type": "element"}
                        if self._hook_id in CUI.state:
                            kwargs['value'] = CUI.state[self._hook_id].get('value')
                        if 'value' in kwargs:
                            if self._hook_id not in CUI.state:
                                CUI.state[self._hook_id] = {}
                            CUI.state[self._hook_id]['value'] = kwargs.get('value')

                CUIElementAtts.__name__ = e_name
                setattr(UI, e_name, CUIElementAtts)
                return CUIElementAtts
        return decorator

    JS_CUI = """
    (function() {
        const PREFIX = '__CUI__';
        const cui = window.cUIData = window.cUIData || {};
        cui.templates = cui.templates || {};
        cui.version = 'vc-manager-browser-recovery-v4';

        const removeElement = function(element) {
            if (!element) return;
            if (typeof element.remove === 'function') element.remove();
            else if (element.parentNode) element.parentNode.removeChild(element);
        };

        const processTemplate = function(template, props) {
            let result = template || '';
            Object.keys(props || {}).forEach(function(key) {
                let value = props[key];
                if (typeof value !== 'string' && typeof value !== 'number') {
                    value = JSON.stringify(value);
                }
                result = result.replace(
                    new RegExp('\\\\$\\\\{data\\\\.' + key + '\\\\}', 'g'),
                    value == null ? '' : String(value)
                );
            });
            return result;
        };

        cui.invokeCallback = async function(hookId, callbackName, value) {
            let lastError = null;
            for (let attempt = 0; attempt < 20; attempt += 1) {
                const bridge = window.pywebview && window.pywebview.api;
                if (bridge && typeof bridge.customCallback === 'function') {
                    try {
                        return await bridge.customCallback('_cui_cb', hookId, callbackName, value);
                    } catch (error) {
                        lastError = error;
                    }
                }
                await new Promise(function(resolve) { setTimeout(resolve, 100); });
            }
            throw lastError || new Error('Nighty callback bridge is not ready');
        };

        const callbackHandler = function(hookId, callbackName) {
            return `(async (element) => {
                try {
                    await window.cUIData.invokeCallback('${hookId}', '${callbackName}', element ? element.value : null);
                } catch (error) {
                    console.error('CUI callback failed:', error);
                }
            })(this)`;
        };

        cui.renderCallable = function(payloadJSON) {
            let payload;
            try {
                payload = JSON.parse(payloadJSON);
            } catch (error) {
                console.error('CUI callable payload is invalid:', error);
                return;
            }

            if (!document.body) {
                setTimeout(function() { cui.renderCallable(payloadJSON); }, 50);
                return;
            }

            let finalHTML = processTemplate(payload.html, payload.props);
            if (payload.create_js) payload.create_js = processTemplate(payload.create_js, payload.props);
            if (payload.remove_js) payload.remove_js = processTemplate(payload.remove_js, payload.props);

            (payload.callbacks || []).forEach(function(callbackName) {
                const removeLogic = payload.remove_js
                    ? `const el = document.getElementById('${payload.hookId}'); if (el) { ${payload.remove_js.replace(/element/g, 'el')}; setTimeout(() => { if (el.remove) el.remove(); else if (el.parentNode) el.parentNode.removeChild(el); }, 200); }`
                    : `const el = document.getElementById('${payload.hookId}'); if (el) { if (el.remove) el.remove(); else if (el.parentNode) el.parentNode.removeChild(el); }`;
                const handler = callbackHandler(payload.hookId, callbackName).replace('})(this)', `    ${removeLogic}\n})(this)`);
                finalHTML = finalHTML.replace(
                    new RegExp('\\\\$\\\\{' + callbackName + '\\\\}', 'g'),
                    handler
                );
            });

            const tempDiv = document.createElement('div');
            tempDiv.innerHTML = finalHTML;
            const userRootElement = tempDiv.firstElementChild;
            const wrapper = document.createElement('div');
            wrapper.id = payload.hookId;

            if (userRootElement) {
                wrapper.style.cssText = userRootElement.style.cssText;
                wrapper.innerHTML = userRootElement.innerHTML;
            } else {
                wrapper.innerHTML = finalHTML;
            }

            const oldWrapper = document.getElementById(payload.hookId);
            if (oldWrapper) removeElement(oldWrapper);
            const targetContainer =
                document.querySelector('div[data-radix-scroll-area-viewport]') ||
                document.querySelector('div.w-full.overflow-auto._scrollbar_1gzhj_1') ||
                document.body;
            targetContainer.appendChild(wrapper);

            if (payload.create_js) {
                try {
                    new Function('element', payload.create_js)(wrapper);
                } catch (error) {
                    console.error('CUI create hook failed:', error);
                }
            }

            if (payload.allowOutsideClick) {
                let pressed = false;
                wrapper.addEventListener('mousedown', function(event) {
                    pressed = event.target === wrapper;
                });
                wrapper.addEventListener('mouseup', function(event) {
                    if (event.target !== wrapper || !pressed) {
                        pressed = false;
                        return;
                    }
                    try {
                        if (payload.remove_js) new Function('element', payload.remove_js)(wrapper);
                    } catch (error) {
                        console.error('CUI remove hook failed:', error);
                    }
                    setTimeout(function() { removeElement(wrapper); }, payload.remove_js ? 200 : 0);
                    pressed = false;
                });
            }
        };

        const normalisePlaceholderText = function(value) {
            return String(value || '')
                .replace(/^[\u200B-\u200D\u2060\uFEFF]+/, '')
                .trim();
        };

        cui.findPlaceholders = function(root) {
            const firstRoot = root && root.nodeType ? root : document;
            const searchRoots = [];
            const rootSeen = typeof Set === 'function' ? new Set() : null;
            const found = [];
            const seen = typeof Set === 'function' ? new Set() : null;

            const addRoot = function(candidate) {
                if (!candidate || !candidate.nodeType) return;
                if (rootSeen && rootSeen.has(candidate)) return;
                if (rootSeen) rootSeen.add(candidate);
                searchRoots.push(candidate);
            };

            addRoot(firstRoot);
            for (let rootIndex = 0; rootIndex < searchRoots.length; rootIndex += 1) {
                const searchRoot = searchRoots[rootIndex];
                const descendants = searchRoot.querySelectorAll ? searchRoot.querySelectorAll('*') : [];
                Array.prototype.forEach.call(descendants, function(element) {
                    if (element.shadowRoot) addRoot(element.shadowRoot);
                    if (String(element.tagName || '').toLowerCase() === 'iframe') {
                        try {
                            addRoot(element.contentDocument);
                        } catch (error) {
                            // Cross-origin frames cannot contain Nighty's tab UI.
                        }
                    }
                });
            }

            searchRoots.forEach(function(searchRoot) {
                const ownerDocument = searchRoot.ownerDocument || searchRoot;
                if (!ownerDocument.createTreeWalker || typeof NodeFilter === 'undefined') {
                    const candidates = searchRoot.querySelectorAll ? searchRoot.querySelectorAll('p, span, div') : [];
                    Array.prototype.forEach.call(candidates, function(element) {
                        if (normalisePlaceholderText(element.textContent).indexOf(PREFIX) !== 0) return;
                        if (!seen || !seen.has(element)) {
                            if (seen) seen.add(element);
                            found.push(element);
                        }
                    });
                    return;
                }

                const walker = ownerDocument.createTreeWalker(
                    searchRoot,
                    NodeFilter.SHOW_TEXT,
                    {
                        acceptNode: function(node) {
                            return normalisePlaceholderText(node.nodeValue).indexOf(PREFIX) === 0
                                ? NodeFilter.FILTER_ACCEPT
                                : NodeFilter.FILTER_REJECT;
                        }
                    }
                );
                let textNode = walker.nextNode();
                while (textNode) {
                    const element = textNode.parentElement;
                    if (element && (!seen || !seen.has(element))) {
                        if (seen) seen.add(element);
                        found.push(element);
                    }
                    textNode = walker.nextNode();
                }
            });

            if (found.length === 0) {
                searchRoots.forEach(function(searchRoot) {
                    const candidates = searchRoot.querySelectorAll
                        ? searchRoot.querySelectorAll('p, span, div, pre, code')
                        : [];
                    Array.prototype.forEach.call(candidates, function(element) {
                        if (normalisePlaceholderText(element.textContent).indexOf(PREFIX) !== 0) return;
                        const childMatch = Array.prototype.some.call(
                            element.children || [],
                            function(child) {
                                return normalisePlaceholderText(child.textContent).indexOf(PREFIX) === 0;
                            }
                        );
                        if (childMatch || (seen && seen.has(element))) return;
                        if (seen) seen.add(element);
                        found.push(element);
                    });
                });
            }
            return found;
        };

        cui.injectPlaceholder = function(placeholder) {
            const raw = normalisePlaceholderText(placeholder.textContent);
            if (raw.indexOf(PREFIX) !== 0) return false;

            let data;
            try {
                data = JSON.parse(raw.slice(PREFIX.length).trim());
            } catch (error) {
                if (!placeholder.dataset || placeholder.dataset.cuiParseLogged !== 'true') {
                    console.error('CUI placeholder contains invalid JSON:', error, raw);
                    if (placeholder.dataset) placeholder.dataset.cuiParseLogged = 'true';
                }
                return false;
            }

            const template = cui.templates[data.type];
            if (!template) return false;
            let finalHTML = processTemplate(template, data.props || {});
            (data.callbacks || []).forEach(function(callbackName) {
                finalHTML = finalHTML.replace(
                    new RegExp('\\\\$\\\\{' + callbackName + '\\\\}', 'g'),
                    callbackHandler(data.hook, callbackName)
                );
            });

            const holder = document.createElement('div');
            holder.innerHTML = finalHTML.trim();
            const fragment = document.createDocumentFragment();
            while (holder.firstChild) fragment.appendChild(holder.firstChild);
            if (!placeholder.parentNode) return false;
            placeholder.parentNode.replaceChild(fragment, placeholder);
            return true;
        };

        cui.scanAndInject = function(root) {
            if (cui.scanning) return;
            cui.scanning = true;
            try {
                cui.findPlaceholders(root || document).forEach(function(placeholder) {
                    try {
                        cui.injectPlaceholder(placeholder);
                    } catch (error) {
                        console.error('CUI could not render a custom element:', error);
                    }
                });
            } finally {
                cui.scanning = false;
            }
        };

        cui.scheduleScan = function(root) {
            if (cui.scanScheduled) return;
            cui.scanScheduled = true;
            let finished = false;
            const run = function() {
                if (finished) return;
                finished = true;
                cui.scanScheduled = false;
                cui.scanAndInject(root || document);
            };
            if (typeof window.requestAnimationFrame === 'function' && document.visibilityState !== 'hidden') {
                window.requestAnimationFrame(run);
                setTimeout(run, 160);
            } else {
                setTimeout(run, 0);
            }
        };

        cui.updateTemplates = function(newTemplates) {
            Object.assign(cui.templates, newTemplates || {});
            cui.scheduleScan(document);
        };

        cui.installRecovery = function() {
            const target = document.documentElement || document.body;
            if (!target) {
                setTimeout(cui.installRecovery, 50);
                return;
            }

            if (cui.observer && typeof cui.observer.disconnect === 'function') cui.observer.disconnect();
            if (typeof MutationObserver === 'function') {
                cui.observer = new MutationObserver(function() { cui.scheduleScan(document); });
                cui.observer.observe(target, { childList: true, subtree: true, characterData: true });
            }

            if (cui.recoveryTimer) clearInterval(cui.recoveryTimer);
            cui.recoveryTimer = setInterval(function() { cui.scanAndInject(document); }, 1000);

            if (cui.recoveryHandler) {
                window.removeEventListener('load', cui.recoveryHandler);
                window.removeEventListener('pageshow', cui.recoveryHandler);
                window.removeEventListener('focus', cui.recoveryHandler);
                window.removeEventListener('online', cui.recoveryHandler);
                window.removeEventListener('popstate', cui.recoveryHandler);
                window.removeEventListener('hashchange', cui.recoveryHandler);
                window.removeEventListener('orientationchange', cui.recoveryHandler);
                window.removeEventListener('resume', cui.recoveryHandler);
                document.removeEventListener('visibilitychange', cui.recoveryHandler);
                document.removeEventListener('webkitvisibilitychange', cui.recoveryHandler);
                document.removeEventListener('readystatechange', cui.recoveryHandler);
            }
            cui.recoveryHandler = function() { cui.scheduleScan(document); };
            window.addEventListener('load', cui.recoveryHandler);
            window.addEventListener('pageshow', cui.recoveryHandler);
            window.addEventListener('focus', cui.recoveryHandler);
            window.addEventListener('online', cui.recoveryHandler);
            window.addEventListener('popstate', cui.recoveryHandler);
            window.addEventListener('hashchange', cui.recoveryHandler);
            window.addEventListener('orientationchange', cui.recoveryHandler);
            window.addEventListener('resume', cui.recoveryHandler);
            document.addEventListener('visibilitychange', cui.recoveryHandler);
            document.addEventListener('webkitvisibilitychange', cui.recoveryHandler);
            document.addEventListener('readystatechange', cui.recoveryHandler);
            cui.scheduleScan(document);
        };

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', cui.installRecovery, { once: true });
        } else {
            cui.installRecovery();
        }
    })();
    """

    def get_cui_templates():
        templates = {}
        for name, definition in CUI.registry.items():
            if definition["type"] != "element":
                continue
            valid_params = definition["init_params"]
            empty_kwargs = {param: None for param in valid_params if param != "self"}
            templates[name] = definition["class"](**empty_kwargs).html
        return templates

    def build_cui_bootstrap_js(include_helpers=False):
        """Build one ordered payload so slower browsers cannot miss CUI templates."""
        scripts = [
            "(function(){if(!window.cUIData||"
            "window.cUIData.version!=='vc-manager-browser-recovery-v4'){"
            + JS_CUI
            + "}else{window.cUIData.installRecovery();}})();"
        ]
        templates = get_cui_templates()
        if templates:
            scripts.append(
                "window.cUIData.updateTemplates(" + json.dumps(templates) + ");"
            )

        if include_helpers:
            try:
                scripts.append(CardPager.pg_js())
            except (NameError, AttributeError):
                pass
            try:
                scripts.append(VC_UI_JS)
            except NameError:
                pass

        scripts.append("""
            (function() {
                function restoreCUI() {
                    if (!window.cUIData) return;
                    try {
                        window.cUIData.installRecovery();
                        window.cUIData.scanAndInject(document);
                    } catch (error) {
                        console.error('VC Manager CUI recovery failed:', error);
                    }
                }

                restoreCUI();
                [50, 150, 400, 1000, 2500, 5000].forEach(function(delay) {
                    setTimeout(restoreCUI, delay);
                });

                if (!window.__vcManagerCUIRecoveryEvents) {
                    window.__vcManagerCUIRecoveryEvents = true;
                    window.addEventListener('load', restoreCUI);
                    window.addEventListener('pageshow', restoreCUI);
                    window.addEventListener('online', restoreCUI);
                    document.addEventListener('visibilitychange', restoreCUI);
                }
            })();
        """)
        return "\n".join(scripts)

    def _cui_cb(hook_id, func_name, value):
        ins_data = CUI.instances.get(hook_id)
        if ins_data:
            try:
                kwargs = ins_data['kwargs']
                cb_func = kwargs.get(func_name)

                if cb_func:
                    if value is None:
                        try:
                            return cb_func()
                        except TypeError as e:
                            err_msg = str(e)
                            if any(phrase in err_msg for phrase in ["missing", "required", "positional", "takes exactly", "takes from"]):
                                return cb_func(None)
                            raise
                    else:
                        try:
                            return cb_func(str(value))
                        except TypeError as e:
                            err_msg = str(e)
                            if any(phrase in err_msg for phrase in ["takes 0", "takes no", "0 positional", "takes exactly 0"]):
                                return cb_func()
                            raise
            except Exception as e:
                print(f"CUI Error: Failed to execute cb '{func_name}', error: {e}", type_="ERROR")
                return None

    def patch_ui_class(cls):
        if not hasattr(cls, '_o_cuie'):
            cls._o_cuie = cls.create_ui_element
        def p_cuie(self, e_type, **kwargs):
            if hasattr(e_type, "__name__") and CUI.registry.get(e_type.__name__, {}).get("type") == "element":
                instance = e_type(**kwargs)
                ins_data = CUI.instances[instance._hook_id]
                template_props = CUI.registry[e_type.__name__]['init_params']
                ins_kwargs = ins_data['kwargs']
                filtered_props = {k: v for k, v in ins_kwargs.items() if k in template_props and not callable(v)}
                cb_names = [k for k, v in ins_kwargs.items() if k in template_props and callable(v)]
                payload = {
                    "type": e_type.__name__,
                    "hook": instance._hook_id,
                    "props": filtered_props,
                    "callbacks": cb_names
                }
                dc_content = f"__CUI__{json.dumps(payload)}"
                return self._o_cuie(UI.Text, content=dc_content)
            else:
                return self._o_cuie(e_type, **kwargs)
        cls.create_ui_element = p_cuie

    patch_ui_class(Group)
    patch_ui_class(Card)

    if not hasattr(Tab, '_o_render'):
        Tab._o_render = Tab.render
    def p_render(self):
        self._o_render()
        callback_store.register(_cui_cb)
        evaluate_js(build_cui_bootstrap_js(include_helpers=True))
    Tab.render = p_render

    def reeval():
        try:
            # Keep the bootstrap and templates in one browser evaluation. Mobile
            # tabs can process separate Socket.IO messages out of order while a
            # page is still mounting.
            evaluate_js(build_cui_bootstrap_js(include_helpers=True))

            # Send current rows and connection state to the new page.
            try:
                update_all_ui()
                update_token_table_rows()
            except NameError:
                pass
        except Exception as e:
            print(f"[VC Manager] Could not restore the custom UI after reload: {e}", type_="ERROR")

    if not getattr(socketio, "_is_multi_listener_patched", False):
        _orig_socketio_on = socketio.on
        socketio._event_registry = {}
        socketio._is_multi_listener_patched = True

        def _patched_on(message, namespace=None):
            def decorator(func):
                reg_key = (message, namespace)

                if reg_key not in socketio._event_registry:
                    socketio._event_registry[reg_key] = []

                    def _master_dispatcher(*args, **kwargs):
                        for hand in list(socketio._event_registry[reg_key]):
                            try:
                                sig = inspect.signature(hand)
                                params = sig.parameters
                                has_var_args = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values())
                                has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
                                num_pos = sum(1 for p in params.values() if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD))

                                call_args = args if has_var_args else args[:num_pos]
                                call_kwargs = kwargs if has_var_kw else {k: v for k, v in kwargs.items() if k in params}

                                hand(*call_args, **call_kwargs)
                            except Exception as e:
                                print(f"SIO Registry: Error running listener for '{message}': {e}", type_="ERROR")

                    if namespace:
                        _orig_socketio_on(message, namespace=namespace)(_master_dispatcher)
                    else:
                        _orig_socketio_on(message)(_master_dispatcher)

                if func not in socketio._event_registry[reg_key]:
                    socketio._event_registry[reg_key].append(func)

                return func
            return decorator

        socketio.on = _patched_on

    # The socket registry survives script replacement. Remove only listeners
    # owned by an older VC Manager generation before registering this one.
    connect_key = ("connect", None)
    if hasattr(socketio, "_event_registry"):
        socketio._event_registry[connect_key] = [
            handler for handler in socketio._event_registry.get(connect_key, [])
            if not getattr(handler, "_vc_manager_socket_handler", False)
            and getattr(handler, "__name__", "") not in {"con", "restore_vc_custom_ui", "vc_manager_cui_connect", "vc_manager_ui_connect"}
        ]

    @socketio.on("connect")
    def vc_manager_cui_connect():
    # Some embedded browsers reuse a Socket.IO session after a refresh.
    # Reinstall CUI so the page does not show raw __CUI__ placeholders.
        reeval()

    vc_manager_cui_connect._vc_manager_socket_handler = True

    @cui_new(type="element")
    class PageMarker:
        _init_params = ["self", "page_id", "pager_id", "max_height"]

        def __init__(self, page_id=None, pager_id=None, max_height=None):
            self.html = '<div id="${data.page_id}" class="cui-page-marker" data-pager-id="${data.pager_id}" data-max-height="${data.max_height}" style="display: none !important;"></div>'

    @cui_new(type="element")
    class Space:
        _init_params = ["self", "height"]

        def __init__(self, height="10px"):
            self.html = '<div style="display: block; width: 100%; height: ${data.height}; min-height: ${data.height}; pointer-events: none;"></div>'

    @cui_new(type="element")
    class Slider:
        _init_params = ["self", "label", "min_val", "max_val", "value", "on_change"]

        def __init__(self, label="Default", min_val=0, max_val=100, value=50, on_change=None):
            self.funcs = [on_change]
            self.html = """
                <style>
                    .vc-timer-slider {
                        width: 100%;
                        height: 4px;
                        margin: 0;
                        appearance: none;
                        -webkit-appearance: none;
                        border-radius: 999px;
                        background: rgba(148, 163, 184, 0.28);
                        cursor: pointer;
                        outline: none;
                    }
                    .vc-timer-slider::-webkit-slider-thumb {
                        width: 16px;
                        height: 16px;
                        appearance: none;
                        -webkit-appearance: none;
                        border: 3px solid #4a6d88;
                        border-radius: 50%;
                        background: #f8fafc;
                        box-shadow: 0 1px 4px rgba(8, 15, 28, 0.45);
                        transition: border-color 160ms ease, transform 160ms ease;
                    }
                    .vc-timer-slider::-moz-range-thumb {
                        width: 10px;
                        height: 10px;
                        border: 3px solid #4a6d88;
                        border-radius: 50%;
                        background: #f8fafc;
                        box-shadow: 0 1px 4px rgba(8, 15, 28, 0.45);
                        transition: border-color 160ms ease, transform 160ms ease;
                    }
                    .vc-timer-slider::-moz-range-progress {
                        height: 4px;
                        border-radius: 999px;
                        background: #4a6d88;
                    }
                    .vc-timer-slider:hover::-webkit-slider-thumb,
                    .vc-timer-slider:focus-visible::-webkit-slider-thumb {
                        border-color: #6f91ab;
                        transform: scale(1.08);
                    }
                    .vc-timer-slider:hover::-moz-range-thumb,
                    .vc-timer-slider:focus-visible::-moz-range-thumb {
                        border-color: #6f91ab;
                        transform: scale(1.08);
                    }
                    .vc-timer-slider:focus-visible {
                        box-shadow: 0 0 0 3px rgba(74, 109, 136, 0.24);
                    }
                </style>
                <div style="flex: 1 1 auto; display: flex; flex-direction: column; gap: 9px; width: 100%; min-width: 180px; padding-right: 8px;">
                    <div style="display: flex; align-items: baseline; justify-content: space-between; gap: 12px;">
                        <label style="font-size: 0.8125rem; font-weight: 600;">
                            ${data.label}
                        </label>
                        <span data-slider-value style="min-width: 58px; padding: 3px 7px; border: 1px solid rgba(148, 163, 184, 0.22); border-radius: 6px; background: rgba(15, 23, 42, 0.32); font-family: monospace; font-size: 0.75rem; font-variant-numeric: tabular-nums; text-align: center;">
                            ${data.value} min
                        </span>
                    </div>
                    <input class="vc-timer-slider"
                           type="range"
                           min="${data.min_val}" max="${data.max_val}" value="${data.value}"
                           aria-label="${data.label}"
                           oninput="this.parentElement.querySelector('[data-slider-value]').textContent = this.value === '0' ? 'Off' : this.value + ' min';"
                           onchange="${on_change}">
                    <div style="display: flex; justify-content: space-between; font-family: monospace; font-size: 0.6875rem; font-variant-numeric: tabular-nums; opacity: 0.62;">
                        <span>Off</span>
                        <span>2 hr</span>
                    </div>
                </div>
            """

    @cui_new(type="element")
    class ParticipantPager:
        _init_params = ["self", "pager_id"]

        def __init__(self, pager_id=None):
            self.html = """
                <div id="${data.pager_id}" class="vc-participant-pager" style="display: flex; flex: 1 1 auto; min-height: 0; width: 100%;">
                    <div data-participant-controls style="position: absolute; top: 16px; right: 16px; display: flex; align-items: center; gap: 8px; background: rgba(30, 30, 30, 0.7); backdrop-filter: blur(4px); padding: 4px 8px; border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.1); z-index: 10;">
                        <span data-participant-counter style="font-family: monospace; font-size: 14px;">1 / 1</span>
                        <button type="button" data-participant-prev onclick="window.VCParticipantPager.move('${data.pager_id}', -1)" style="background: none; border: none; cursor: pointer; font-size: 24px; padding: 0 5px; line-height: 1;">&lsaquo;</button>
                        <button type="button" data-participant-next onclick="window.VCParticipantPager.move('${data.pager_id}', 1)" style="background: none; border: none; cursor: pointer; font-size: 24px; padding: 0 5px; line-height: 1;">&rsaquo;</button>
                    </div>
                    <div style="display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0; width: 100%; padding: 12px; background: rgba(255,255,255,0.035); border-radius: 10px;">
                        <div style="display: grid; grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr); gap: 12px; padding: 10px 12px; background: rgba(255,255,255,0.07); border-radius: 8px; font-weight: 700;">
                            <span>Participant</span>
                            <span>Voice State</span>
                        </div>
                        <div data-participant-rows style="display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0;"></div>
                    </div>
                </div>
            """

    @cui_new(type="element")
    class ManagedAccountsPager:
        _init_params = ["self", "pager_id", "on_action"]

        def __init__(self, pager_id=None, on_action=None):
            self.funcs = [on_action]
            self.html = """
                <div id="${data.pager_id}" class="vc-managed-accounts-pager" style="display: flex; flex: 1 1 auto; min-height: 0; width: 100%;">
                    <div data-managed-controls style="position: absolute; top: 16px; right: 16px; display: flex; align-items: center; gap: 8px; background: rgba(30, 30, 30, 0.7); backdrop-filter: blur(4px); padding: 4px 8px; border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.1); z-index: 10;">
                        <span data-managed-counter style="font-family: monospace; font-size: 14px;">1 / 1</span>
                        <button type="button" data-managed-prev onclick="window.VCManagedAccountsPager.move('${data.pager_id}', -1)" style="background: none; border: none; cursor: pointer; font-size: 24px; padding: 0 5px; line-height: 1;">&lsaquo;</button>
                        <button type="button" data-managed-next onclick="window.VCManagedAccountsPager.move('${data.pager_id}', 1)" style="background: none; border: none; cursor: pointer; font-size: 24px; padding: 0 5px; line-height: 1;">&rsaquo;</button>
                    </div>
                    <button type="button" data-managed-action-proxy onclick="${on_action}" style="display: none;"></button>
                    <div style="display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0; width: 100%; padding: 12px; background: rgba(255,255,255,0.035); border-radius: 10px;">
                        <div class="vc-managed-header" style="display:grid;grid-template-columns:minmax(140px,1.15fr) minmax(90px,.65fr) minmax(90px,.65fr) minmax(82px,.5fr) minmax(250px,1.25fr) 90px 196px;gap:10px;padding:10px 12px;background:rgba(255,255,255,0.07);border-radius:8px;font-weight:700;">
                            <span>Account</span>
                            <span>Server</span>
                            <span>Channel</span>
                            <span>Status</span>
                            <span>Voice Controls</span>
                            <span>Sessions</span>
                            <span>Action</span>
                        </div>
                        <div data-managed-rows style="display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0;"></div>
                    </div>
                </div>
            """

    class CardPager:
        _jsinj = False

        def __init__(self, card, max_height=280):
            self.card = card
            self.pager_id = f"cui-pager-{id(self)}"
            self.pages = []
            self.max_height = max_height

            if not CardPager._jsinj:
                evaluate_js(self.pg_js())
                CardPager._jsinj = True

        def create_page(self):
            page_id = f"{self.pager_id}-page-{len(self.pages)}"
            page_group = self.card.create_group(type="rows", gap=4, full_width=True)
            page_group.create_ui_element(UI.PageMarker, page_id=page_id, pager_id=self.pager_id, max_height=self.max_height)
            self.pages.append(page_id)
            return page_group

        def build(self):
            if not self.pages:
                return
            evaluate_js("if (window.CUI_Pager) window.CUI_Pager.initPagers();")

        @staticmethod
        def pg_js():
            return """
                (function() {
                    if (
                        window.CUI_Pager &&
                        window.CUI_Pager.version === 'vc-manager-pager-v3' &&
                        window.CUI_Pager.observer
                    ) {
                        window.CUI_Pager.initPagers();
                        return;
                    }
                    if (window.CUI_Pager && window.CUI_Pager.observer) {
                        window.CUI_Pager.observer.disconnect();
                    }
                    delete window.CUI_Pager;

                    const oldStyle = document.getElementById('vc-manager-pager-style');
                    if (oldStyle) oldStyle.remove();

                    const style = document.createElement('style');
                    style.id = 'vc-manager-pager-style';
                    style.textContent = `
                        .cui-pager-wrapper { position: relative; overflow: hidden; width: 100%; }
                        .cui-page {
                            width: 100%;
                            height: auto;
                            position: absolute;
                            top: 0;
                            left: 0;
                            transition: transform 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease-in-out;
                            transform: translateX(100%);
                            opacity: 0;
                            pointer-events: none;
                            box-sizing: border-box;
                        }
                        .cui-pager-controls {
                            position: absolute !important;
                            top: 16px !important;
                            right: 16px !important;
                            z-index: 10 !important;
                            display: inline-flex !important;
                            flex-direction: row !important;
                            flex-wrap: nowrap !important;
                            align-items: center !important;
                            justify-content: center !important;
                            gap: 6px !important;
                            width: auto !important;
                            min-width: 0 !important;
                            max-width: calc(100% - 32px) !important;
                            height: 36px !important;
                            margin: 0 !important;
                            padding: 3px 7px !important;
                            box-sizing: border-box !important;
                            white-space: nowrap !important;
                        }
                        .cui-pager-controls > span {
                            display: inline-block !important;
                            width: auto !important;
                            min-width: 38px !important;
                            flex: 0 0 auto !important;
                            line-height: 1 !important;
                            white-space: nowrap !important;
                        }
                        .cui-pager-controls > button {
                            display: inline-flex !important;
                            align-items: center !important;
                            justify-content: center !important;
                            flex: 0 0 24px !important;
                            width: 24px !important;
                            min-width: 24px !important;
                            max-width: 24px !important;
                            height: 28px !important;
                            min-height: 28px !important;
                            max-height: 28px !important;
                            margin: 0 !important;
                            padding: 0 !important;
                        }
                        @media (max-width: 640px) {
                            .cui-pager-controls {
                                top: 14px !important;
                                right: 12px !important;
                                gap: 3px !important;
                                height: 32px !important;
                                padding: 2px 5px !important;
                                max-width: calc(100% - 24px) !important;
                            }
                            .cui-pager-controls > span {
                                min-width: 34px !important;
                                font-size: 12px !important;
                            }
                            .cui-pager-controls > button {
                                flex-basis: 22px !important;
                                width: 22px !important;
                                min-width: 22px !important;
                                max-width: 22px !important;
                                height: 26px !important;
                                min-height: 26px !important;
                                max-height: 26px !important;
                                font-size: 20px !important;
                            }
                        }
                    `;
                    document.head.appendChild(style);

                    // Keep pagers that were already rendered by an older
                    // script generation, and apply the compact mobile layout.
                    document.querySelectorAll('.cui-pager-wrapper').forEach(wrapper => {
                        const counter = document.getElementById(`${wrapper.id}-counter`);
                        if (counter && counter.parentElement) {
                            counter.parentElement.classList.add('cui-pager-controls');
                        }
                    });

                    window.CUI_Pager = {
                        version: 'vc-manager-pager-v3',
                        initPagers: function() {
                            const allMarkers = document.querySelectorAll('.cui-page-marker');
                            const pagersToInit = new Map();

                            allMarkers.forEach(marker => {
                                const pagerId = marker.dataset.pagerId;
                                if (document.getElementById(pagerId)) return;

                                if (!pagersToInit.has(pagerId)) {
                                    pagersToInit.set(pagerId, []);
                                }
                                pagersToInit.get(pagerId).push(marker);
                            });

                            pagersToInit.forEach((markers, pagerId) => {
                                window.CUI_Pager.initialize(pagerId, markers[0]);
                            });
                        },

                        initialize: function(pagerId, sourceMarker) {
                            const maxHeight = parseInt(sourceMarker.dataset.maxHeight, 10);
                            const sourceContainer = sourceMarker.parentElement;

                            if (!maxHeight || sourceContainer.scrollHeight <= maxHeight + 20) {
                                sourceMarker.remove();
                                return;
                            }

                            const parentCard = sourceContainer.closest('.card') || sourceContainer.parentElement;
                            if (!parentCard) return;
                            parentCard.style.position = 'relative';

                            const wrapper = document.createElement('div');
                            wrapper.id = pagerId;
                            wrapper.className = 'cui-pager-wrapper';
                            wrapper.dataset.currentPage = '0';

                            parentCard.insertBefore(wrapper, sourceContainer);

                            const allItems = Array.from(sourceContainer.children).filter(el => el !== sourceMarker);
                            let pages = [];
                            const TOLERANCE = 15;

                            if (allItems.length > 0) {
                                let currentPage = document.createElement('div');
                                currentPage.className = sourceContainer.className;
                                currentPage.classList.add('cui-page');
                                currentPage.style.cssText = sourceContainer.style.cssText;
                                currentPage.style.height = 'auto';
                                currentPage.style.maxHeight = 'none';

                                wrapper.appendChild(currentPage);
                                pages.push(currentPage);

                                allItems.forEach(item => {
                                    currentPage.appendChild(item);

                                    if (currentPage.scrollHeight > (maxHeight + TOLERANCE) && currentPage.children.length > 1) {
                                        currentPage.removeChild(item);

                                        currentPage = document.createElement('div');
                                        currentPage.className = sourceContainer.className;
                                        currentPage.classList.add('cui-page');
                                        currentPage.style.cssText = sourceContainer.style.cssText;
                                        currentPage.style.height = 'auto';
                                        currentPage.style.maxHeight = 'none';

                                        wrapper.appendChild(currentPage);
                                        currentPage.appendChild(item);
                                        pages.push(currentPage);
                                    }
                                });
                            }

                            sourceContainer.remove();
                            wrapper.dataset.totalPages = pages.length;

                            if (pages.length <= 1) {
                                if (pages.length === 1) {
                                    const singlePage = pages[0];
                                    singlePage.style.transition = 'none';
                                    singlePage.style.transform = 'translateX(0%)';
                                    singlePage.style.opacity = '1';
                                    singlePage.style.pointerEvents = 'auto';
                                    singlePage.style.position = 'relative';
                                    wrapper.style.height = `${singlePage.scrollHeight}px`;
                                    wrapper.style.position = 'static';
                                } else {
                                    wrapper.style.height = '0px';
                                }
                                return;
                            }

                            const firstPage = pages[0];
                            const setStableHeight = () => {
                                const stableHeight = pages.reduce((height, page) => {
                                    return Math.max(height, page.scrollHeight);
                                }, 0);
                                wrapper.style.height = `${stableHeight}px`;
                            };
                            if (firstPage) {
                                firstPage.style.transition = 'none';
                                firstPage.style.transform = 'translateX(0%)';
                                firstPage.style.opacity = '1';
                                firstPage.style.pointerEvents = 'auto';
                                void firstPage.offsetHeight;
                                firstPage.style.transition = '';
                                setStableHeight();
                            }

                            const btnStyle = "background: none; border: none; cursor: pointer; font-size: 24px; padding: 0 5px; line-height: 1; opacity: 0.75; transition: opacity 0.2s ease;";
                            const hoverEffect = "this.style.opacity='1'";
                            const mouseOutEffect = "this.style.opacity='0.75'";

                            const newControlsHtml = `
                                <div class="cui-pager-controls" style="background-color: rgba(30, 30, 30, 0.7); backdrop-filter: blur(4px); border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.1);">
                                    <span id="${pagerId}-counter" style="font-family: monospace; font-size: 14px;"></span>
                                    <button id="${pagerId}-prev" onclick="CUI_Pager.goToPage('${pagerId}', -1, true)" style="${btnStyle}" onmouseover="${hoverEffect}" onmouseout="${mouseOutEffect}">&lsaquo;</button>
                                    <button id="${pagerId}-next" onclick="CUI_Pager.goToPage('${pagerId}', 1, true)" style="${btnStyle}" onmouseover="${hoverEffect}" onmouseout="${mouseOutEffect}">&rsaquo;</button>
                                </div>
                            `;

                            parentCard.insertAdjacentHTML('afterbegin', newControlsHtml);

                            if (typeof ResizeObserver === 'function') {
                                const resizeObserver = new ResizeObserver(() => {
                                    setStableHeight();
                                });
                                pages.forEach(p => resizeObserver.observe(p));
                            }
                            this.updateControls(pagerId);
                        },

                        goToPage: function(pagerId, target, isRelative = false) {
                            const wrapper = document.getElementById(pagerId);
                            if (!wrapper) return;

                            const pages = Array.from(wrapper.querySelectorAll('.cui-page'));
                            const totalPages = parseInt(wrapper.dataset.totalPages, 10);
                            let currentIndex = parseInt(wrapper.dataset.currentPage, 10);

                            let targetIndex = isRelative ? currentIndex + target : target;
                            if (targetIndex < 0 || targetIndex >= totalPages || targetIndex === currentIndex) return;

                            const currentPageEl = pages[currentIndex];
                            const targetPageEl = pages[targetIndex];
                            const direction = targetIndex > currentIndex ? 1 : -1;

                            if (currentPageEl) {
                                currentPageEl.style.transform = `translateX(${-100 * direction}%)`;
                                currentPageEl.style.opacity = '0';
                                currentPageEl.style.pointerEvents = 'none';
                            }

                            if (targetPageEl) {
                                targetPageEl.style.transform = `translateX(${100 * direction}%)`;
                                const animate = window.requestAnimationFrame || function(callback) { setTimeout(callback, 0); };
                                animate(() => {
                                   targetPageEl.style.transform = 'translateX(0%)';
                                   targetPageEl.style.opacity = '1';
                                   targetPageEl.style.pointerEvents = 'auto';
                                });
                            }

                            wrapper.dataset.currentPage = targetIndex;
                            this.updateControls(pagerId);
                        },

                        updateControls: function(pagerId) {
                            const wrapper = document.getElementById(pagerId);
                            const counter = document.getElementById(`${pagerId}-counter`);
                            const prevBtn = document.getElementById(`${pagerId}-prev`);
                            const nextBtn = document.getElementById(`${pagerId}-next`);

                            if (!wrapper || !counter || !prevBtn || !nextBtn) return;

                            const currentIndex = parseInt(wrapper.dataset.currentPage, 10);
                            const totalPages = parseInt(wrapper.dataset.totalPages, 10);

                            counter.textContent = `${currentIndex + 1} / ${totalPages}`;
                            prevBtn.disabled = currentIndex === 0;
                            nextBtn.disabled = currentIndex >= totalPages - 1;

                            prevBtn.style.opacity = prevBtn.disabled ? '0.4' : '1';
                            nextBtn.style.opacity = nextBtn.disabled ? '0.4' : '1';
                            prevBtn.style.cursor = prevBtn.disabled ? 'not-allowed' : 'pointer';
                            nextBtn.style.cursor = nextBtn.disabled ? 'not-allowed' : 'pointer';
                        }
                    };

                    if (typeof MutationObserver === 'function' && (document.documentElement || document.body)) {
                        const observer = new MutationObserver(window.CUI_Pager.initPagers);
                        observer.observe(document.documentElement || document.body, { childList: true, subtree: true });
                        window.CUI_Pager.observer = observer;
                    }
                    window.CUI_Pager.initPagers();
                })();
            """

    # ==================== CUI CUSTOM COMPONENTS ====================

    VC_UI_JS = """
    (function() {
        const uiVersion = 'vc-manager-ui-v5';
        if (
            window.VCManagerUIVersion === uiVersion &&
            (window.VCManagerLayoutObserver || window.VCManagerLayoutTimer)
        ) {
            if (typeof window.VCManagerRefreshLayout === 'function') {
                window.VCManagerRefreshLayout();
            }
            return;
        }
        window.VCManagerUIVersion = uiVersion;

        const clearElement = function(element) {
            if (!element) return;
            if (typeof element.replaceChildren === 'function') element.replaceChildren();
            else while (element.firstChild) element.removeChild(element.firstChild);
        };

        // Remove the banner left in an already-open tab by pre-fix versions.
        const staleBanner = document.getElementById('vc_header_banner_root');
        if (staleBanner) {
            const staleBannerCard = staleBanner.closest('.card');
            if (staleBannerCard) staleBannerCard.remove();
            else staleBanner.remove();
        }
        const staleVisualizer = document.getElementById('vc_audio_viz_root');
        if (staleVisualizer) staleVisualizer.remove();

        let responsiveStyle = document.getElementById('vc-manager-responsive-style');
        if (!responsiveStyle) {
            responsiveStyle = document.createElement('style');
            responsiveStyle.id = 'vc-manager-responsive-style';
            document.head.appendChild(responsiveStyle);
        }
        responsiveStyle.textContent = `
            .vc-managed-row, .vc-participant-pager [data-participant-rows] > div {
                transition: background-color 160ms ease, border-color 160ms ease;
            }
            .vc-managed-row:hover { background: rgba(255,255,255,.025); }
            .vc-managed-row button:focus-visible,
            .vc-managed-accounts-pager button:focus-visible,
            .vc-participant-pager button:focus-visible {
                outline: 2px solid rgba(111,145,171,.92);
                outline-offset: 2px;
            }
            @media (max-width: 1080px) {
                .vc-managed-header { display: none !important; }
                .vc-managed-row {
                    display: grid !important;
                    grid-template-columns: minmax(0,1fr) minmax(0,1fr) !important;
                    gap: 10px 14px !important;
                    min-height: 0 !important;
                    margin-bottom: 10px;
                    padding: 14px !important;
                    border: 1px solid rgba(255,255,255,.08);
                    border-radius: 10px;
                }
                .vc-managed-controls, .vc-managed-sessions, .vc-managed-actions {
                    grid-column: 1 / -1;
                    flex-wrap: wrap !important;
                }
                .vc-managed-sessions button { width: auto !important; min-width: 96px; }
            }
            @media (max-width: 640px) {
                .vc-managed-accounts-pager, .vc-participant-pager { min-width: 0 !important; }
                .vc-managed-row { grid-template-columns: minmax(0,1fr) !important; }
                .vc-managed-row > * { grid-column: 1 !important; }
                .vc-managed-controls { gap: 5px !important; }
                .vc-managed-controls button { flex: 1 1 calc(50% - 5px); }
                .vc-managed-sessions button { width: 100% !important; }
                .vc-managed-actions button { min-width: 0 !important; }
                .vc-participant-pager [data-participant-controls],
                .vc-managed-accounts-pager [data-managed-controls] {
                    top: 12px !important;
                    right: 12px !important;
                    padding: 3px 6px !important;
                }
            }
        `;

        const participantPager = window.VCParticipantPager || { states: {} };
        participantPager.states = participantPager.states || {};
        participantPager.pageSize = 5;
        participantPager.update = function(pagerId, rows) {
            const previous = this.states[pagerId] || { page: 0 };
            const nextRows = Array.isArray(rows) ? rows : [];
            const oldKey = (previous.rows || []).map(item => String(item.id || '')).join('|');
            const newKey = nextRows.map(item => String(item.id || '')).join('|');
            previous.rows = nextRows;
            if (oldKey !== newKey) previous.page = 0;
            const totalPages = Math.max(1, Math.ceil(previous.rows.length / this.pageSize));
            previous.page = Math.min(Math.max(0, previous.page || 0), totalPages - 1);
            this.states[pagerId] = previous;
            this.render(pagerId);
        };
        participantPager.move = function(pagerId, direction) {
            const state = this.states[pagerId] || { rows: [], page: 0 };
            const totalPages = Math.max(1, Math.ceil(state.rows.length / this.pageSize));
            state.page = Math.min(Math.max(0, state.page + direction), totalPages - 1);
            this.states[pagerId] = state;
            this.render(pagerId);
        };
        participantPager.render = function(pagerId) {
            const root = document.getElementById(pagerId);
            if (!root) return;

            const state = this.states[pagerId] || { rows: [], page: 0 };
            const rows = Array.isArray(state.rows) ? state.rows : [];
            const totalPages = Math.max(1, Math.ceil(rows.length / this.pageSize));
            state.page = Math.min(Math.max(0, state.page || 0), totalPages - 1);
            this.states[pagerId] = state;

            const counter = root.querySelector('[data-participant-counter]');
            const prev = root.querySelector('[data-participant-prev]');
            const next = root.querySelector('[data-participant-next]');
            const body = root.querySelector('[data-participant-rows]');
            if (!counter || !prev || !next || !body) return;

            counter.textContent = `${state.page + 1} / ${totalPages}`;
            prev.disabled = state.page === 0;
            next.disabled = state.page >= totalPages - 1;
            prev.style.opacity = prev.disabled ? '0.4' : '1';
            next.style.opacity = next.disabled ? '0.4' : '1';
            prev.style.cursor = prev.disabled ? 'not-allowed' : 'pointer';
            next.style.cursor = next.disabled ? 'not-allowed' : 'pointer';
            clearElement(body);

            const pageRows = rows.slice(state.page * this.pageSize, (state.page + 1) * this.pageSize);
            if (pageRows.length === 0) {
                const empty = document.createElement('div');
                empty.textContent = 'No active users';
                empty.style.cssText = 'display:flex;align-items:center;justify-content:center;flex:1 1 auto;min-height:120px;font-size:13px;opacity:.72;';
                body.appendChild(empty);
                return;
            }

            pageRows.forEach(item => {
                const row = document.createElement('div');
                row.className = 'vc-managed-row';
                row.style.cssText = 'display:grid;grid-template-columns:minmax(0,1.2fr) minmax(0,1fr);gap:12px;align-items:center;min-height:86px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06);box-sizing:border-box;';

                const identity = document.createElement('div');
                identity.style.cssText = 'display:flex;align-items:center;gap:10px;min-width:0;';
                if (item.avatar_url) {
                    const avatar = document.createElement('img');
                    avatar.src = item.avatar_url;
                    avatar.alt = item.name ? `${item.name} avatar` : 'Participant avatar';
                    avatar.style.cssText = 'width:34px;height:34px;border-radius:10px;object-fit:cover;flex:0 0 auto;';
                    identity.appendChild(avatar);
                }

                const identityText = document.createElement('div');
                identityText.style.cssText = 'display:flex;flex-direction:column;min-width:0;';
                const name = document.createElement('span');
                name.textContent = item.name || 'Unknown user';
                name.style.cssText = 'font-size:14px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
                const id = document.createElement('span');
                id.textContent = `ID: ${item.id || '-'}`;
                id.style.cssText = 'font-size:9px;line-height:1.1;opacity:.58;overflow-wrap:anywhere;';
                identityText.append(name, id);
                identity.appendChild(identityText);

                const stateText = document.createElement('span');
                stateText.textContent = item.voice_state || 'Active';
                stateText.style.cssText = 'font-size:13px;font-weight:600;overflow-wrap:anywhere;';
                row.append(identity, stateText);
                body.appendChild(row);
            });
        };
        window.VCParticipantPager = participantPager;

        const managedPager = window.VCManagedAccountsPager || { states: {} };
        managedPager.states = managedPager.states || {};
        managedPager.pageSize = 3;
        managedPager.update = function(pagerId, rows) {
            const previous = this.states[pagerId] || { page: 0 };
            const nextRows = Array.isArray(rows) ? rows : [];
            const oldKey = (previous.rows || []).map(item => String(item.id || '')).join('|');
            const newKey = nextRows.map(item => String(item.id || '')).join('|');
            previous.rows = nextRows;
            if (oldKey !== newKey) previous.page = 0;
            this.states[pagerId] = previous;
            this.render(pagerId);
        };
        managedPager.move = function(pagerId, direction) {
            const state = this.states[pagerId] || { rows: [], page: 0 };
            const totalPages = Math.max(1, Math.ceil(state.rows.length / this.pageSize));
            state.page = Math.min(Math.max(0, state.page + direction), totalPages - 1);
            this.states[pagerId] = state;
            this.render(pagerId);
        };
        managedPager.action = function(pagerId, rowId, actionName) {
            const root = document.getElementById(pagerId);
            const proxy = root ? root.querySelector('[data-managed-action-proxy]') : null;
            if (!proxy) return;
            const payload = JSON.stringify({
                id: String(rowId || ''),
                action: String(actionName || 'account')
            });
            // Keep both forms in sync. Some Nighty browser builds read the
            // current property while others read the serialized attribute.
            proxy.value = payload;
            proxy.setAttribute('value', payload);
            proxy.click();
        };
        managedPager.closeMenus = function() {
            document.querySelectorAll('[data-managed-token-menu]').forEach(menu => {
                menu.style.display = 'none';
            });
        };
        if (!managedPager.dismissHandlerBound) {
            document.addEventListener('click', () => managedPager.closeMenus());
            managedPager.dismissHandlerBound = true;
        }
        managedPager.render = function(pagerId) {
            const root = document.getElementById(pagerId);
            if (!root) return;
            root.dataset.managedPagerReady = 'true';

            const state = this.states[pagerId] || { rows: [], page: 0 };
            const rows = Array.isArray(state.rows) ? state.rows : [];
            const totalPages = Math.max(1, Math.ceil(rows.length / this.pageSize));
            state.page = Math.min(Math.max(0, state.page || 0), totalPages - 1);
            this.states[pagerId] = state;

            const counter = root.querySelector('[data-managed-counter]');
            const prev = root.querySelector('[data-managed-prev]');
            const next = root.querySelector('[data-managed-next]');
            const body = root.querySelector('[data-managed-rows]');
            if (!counter || !prev || !next || !body) return;

            counter.textContent = `${state.page + 1} / ${totalPages}`;
            prev.disabled = state.page === 0;
            next.disabled = state.page >= totalPages - 1;
            prev.style.opacity = prev.disabled ? '0.4' : '1';
            next.style.opacity = next.disabled ? '0.4' : '1';
            prev.style.cursor = prev.disabled ? 'not-allowed' : 'pointer';
            next.style.cursor = next.disabled ? 'not-allowed' : 'pointer';
            clearElement(body);

            const pageRows = rows.slice(state.page * this.pageSize, (state.page + 1) * this.pageSize);
            if (pageRows.length === 0) {
                const empty = document.createElement('div');
                empty.textContent = 'No managed accounts';
                empty.style.cssText = 'display:flex;align-items:center;justify-content:center;flex:1 1 auto;min-height:120px;font-size:13px;opacity:.72;';
                body.appendChild(empty);
                return;
            }

            pageRows.forEach(item => {
                const row = document.createElement('div');
                row.className = 'vc-managed-row';
                row.style.cssText = 'display:grid;grid-template-columns:minmax(140px,1.15fr) minmax(90px,.65fr) minmax(90px,.65fr) minmax(82px,.5fr) minmax(250px,1.25fr) 90px 196px;gap:10px;align-items:center;min-height:76px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06);box-sizing:border-box;';

                const identity = document.createElement('div');
                identity.style.cssText = 'display:flex;align-items:center;gap:10px;min-width:0;';
                if (item.avatar_url) {
                    const avatar = document.createElement('img');
                    avatar.src = item.avatar_url;
                    avatar.alt = item.name ? `${item.name} avatar` : 'Account avatar';
                    avatar.style.cssText = 'width:34px;height:34px;border-radius:10px;object-fit:cover;flex:0 0 auto;';
                    identity.appendChild(avatar);
                } else {
                    const fallback = document.createElement('div');
                    fallback.textContent = (item.name || 'A').trim().charAt(0).toUpperCase();
                    fallback.style.cssText = 'display:flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:10px;background:rgba(255,255,255,.08);font-weight:700;flex:0 0 auto;';
                    identity.appendChild(fallback);
                }

                const identityText = document.createElement('div');
                identityText.style.cssText = 'display:flex;flex-direction:column;min-width:0;';
                const name = document.createElement('span');
                name.textContent = item.name || 'Account';
                name.style.cssText = 'font-size:14px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
                const id = document.createElement('span');
                id.textContent = `ID: ${item.account_id || '-'}`;
                id.style.cssText = 'font-size:9px;line-height:1.1;opacity:.58;overflow-wrap:anywhere;';
                identityText.append(name, id);
                identity.appendChild(identityText);

                const server = document.createElement('span');
                server.textContent = item.server || '-';
                server.title = item.server || '-';
                server.style.cssText = 'font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
                const channel = document.createElement('span');
                channel.textContent = item.channel || '-';
                channel.title = item.channel || '-';
                channel.style.cssText = 'font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
                const status = document.createElement('div');
                status.style.cssText = 'display:flex;flex-direction:column;min-width:0;';
                const statusLabel = document.createElement('span');
                statusLabel.textContent = item.status || 'Idle';
                statusLabel.style.cssText = 'font-size:13px;font-weight:600;';
                status.appendChild(statusLabel);
                if (item.error) {
                    const statusError = document.createElement('span');
                    statusError.textContent = item.error;
                    statusError.title = item.error;
                    statusError.style.cssText = 'font-size:10px;line-height:1.2;opacity:.68;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;';
                    status.appendChild(statusError);
                }

                const controls = document.createElement('div');
                controls.className = 'vc-managed-controls';
                controls.style.cssText = 'display:flex;align-items:center;gap:6px;min-width:0;flex-wrap:nowrap;';
                const controlItems = [
                    ['mute', 'Mute', Boolean(item.muted)],
                    ['deaf', 'Deafen', Boolean(item.deafened)],
                    ['stream', 'Stream', Boolean(item.streaming)],
                    ['video', 'Camera', Boolean(item.camera_on)]
                ];
                controlItems.forEach(([controlName, label, active]) => {
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.textContent = label;
                    button.disabled = !item.connected;
                    button.title = item.connected
                        ? `${active ? 'Disable' : 'Enable'} ${label.toLowerCase()} for ${item.name || 'this account'}`
                        : 'Connect this account before changing its voice controls';
                    button.style.cssText = 'border:1px solid rgba(255,255,255,.14);border-radius:8px;padding:7px 9px;font-size:11px;font-weight:600;white-space:nowrap;';
                    button.style.background = active
                        ? 'rgba(35,145,220,.32)'
                        : 'rgba(255,255,255,.06)';
                    button.style.opacity = button.disabled ? '0.42' : '1';
                    button.style.cursor = button.disabled ? 'not-allowed' : 'pointer';
                    button.onclick = () => this.action(pagerId, item.id, controlName);
                    controls.appendChild(button);
                });

                const sessions = document.createElement('div');
                sessions.className = 'vc-managed-sessions';
                const sessionsButton = document.createElement('button');
                sessionsButton.type = 'button';
                sessionsButton.textContent = 'View Logs';
                sessionsButton.title = `View voice sessions for ${item.name || 'this account'}`;
                sessionsButton.style.cssText = 'width:100%;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);border-radius:8px;padding:7px 9px;font-size:11px;font-weight:600;white-space:nowrap;cursor:pointer;';
                sessionsButton.onclick = () => this.action(pagerId, item.id, 'logs');
                sessions.appendChild(sessionsButton);

                const action = document.createElement('div');
                action.className = 'vc-managed-actions';
                action.style.cssText = 'display:flex;align-items:center;gap:6px;min-width:0;position:relative;';

                const connectionButton = document.createElement('button');
                connectionButton.type = 'button';
                connectionButton.textContent = 'Join';
                connectionButton.disabled = item.removable === false || item.connected;
                connectionButton.style.cssText = 'border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);border-radius:8px;padding:7px 12px;font-size:12px;font-weight:600;cursor:pointer;min-width:72px;';
                connectionButton.style.opacity = connectionButton.disabled ? '0.45' : '1';
                connectionButton.style.cursor = connectionButton.disabled ? 'not-allowed' : 'pointer';
                connectionButton.onclick = () => this.action(pagerId, item.id, 'join');

                const leaveButton = document.createElement('button');
                leaveButton.type = 'button';
                leaveButton.textContent = 'Leave';
                leaveButton.disabled = item.removable === false || !item.connected;
                leaveButton.style.cssText = 'border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);border-radius:8px;padding:7px 12px;font-size:12px;font-weight:600;cursor:pointer;min-width:72px;';
                leaveButton.style.opacity = leaveButton.disabled ? '0.45' : '1';
                leaveButton.style.cursor = leaveButton.disabled ? 'not-allowed' : 'pointer';
                leaveButton.onclick = () => this.action(pagerId, item.id, 'leave');

                const menuButton = document.createElement('button');
                menuButton.type = 'button';
                menuButton.textContent = String.fromCharCode(8942);
                menuButton.title = `More actions for ${item.name || 'this account'}`;
                menuButton.disabled = item.removable === false;
                menuButton.style.cssText = 'display:flex;align-items:center;justify-content:center;width:34px;height:32px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);border-radius:8px;font-size:20px;line-height:1;cursor:pointer;';
                menuButton.style.opacity = menuButton.disabled ? '0.45' : '1';
                menuButton.style.cursor = menuButton.disabled ? 'not-allowed' : 'pointer';

                const tokenMenu = document.createElement('div');
                tokenMenu.dataset.managedTokenMenu = 'true';
                tokenMenu.style.cssText = 'display:none;position:absolute;right:0;top:38px;z-index:30;min-width:138px;padding:5px;background:rgba(20,20,22,.98);border:1px solid rgba(255,255,255,.14);border-radius:9px;box-shadow:0 10px 28px rgba(0,0,0,.38);';

                const removeTokenButton = document.createElement('button');
                removeTokenButton.type = 'button';
                removeTokenButton.textContent = 'Remove Token';
                removeTokenButton.style.cssText = 'width:100%;border:0;background:transparent;border-radius:6px;padding:8px 10px;text-align:left;font-size:12px;font-weight:600;cursor:pointer;';
                removeTokenButton.onclick = event => {
                    event.stopPropagation();
                    tokenMenu.style.display = 'none';
                    this.action(pagerId, item.id, 'remove');
                };
                tokenMenu.onclick = event => event.stopPropagation();
                tokenMenu.appendChild(removeTokenButton);
                menuButton.onclick = event => {
                    event.stopPropagation();
                    const shouldOpen = tokenMenu.style.display === 'none';
                    this.closeMenus();
                    tokenMenu.style.display = shouldOpen ? 'block' : 'none';
                };

                action.append(connectionButton, leaveButton, menuButton, tokenMenu);
                row.append(identity, server, channel, status, controls, sessions, action);
                body.appendChild(row);
            });
        };
        window.VCManagedAccountsPager = managedPager;

        window.sizeVCManagedAccountsCard = function() {
            const title = Array.from(document.querySelectorAll('*')).find(element =>
                element.childElementCount === 0 && element.textContent.trim() === 'Managed Accounts'
            );
            const card = title ? (title.closest('.card') || title.parentElement) : null;
            if (!card) return;
            card.style.position = 'relative';
            card.style.width = '100%';
            card.style.maxWidth = 'none';
            card.style.boxSizing = 'border-box';
            const pagerRoot = card.querySelector('.vc-managed-accounts-pager');
            if (pagerRoot) {
                pagerRoot.style.width = '100%';
                pagerRoot.style.maxWidth = '100%';
                pagerRoot.style.minWidth = '0';
            }
            if (window.innerWidth < 760) {
                card.style.height = '';
                card.style.minHeight = '';
                card.style.maxHeight = '';
                return;
            }
            card.style.height = '360px';
            card.style.minHeight = '360px';
            card.style.maxHeight = '360px';
            card.style.overflow = 'hidden';
        };

        window.sizeVCAccountSetupCards = function() {
            const findCard = exactTitle => {
                const title = Array.from(document.querySelectorAll('*')).find(element =>
                    element.childElementCount === 0 && element.textContent.trim() === exactTitle
                );
                return title ? (title.closest('.card') || title.parentElement) : null;
            };
            const tokenCard = findCard('Multi-Token Accounts');
            const destinationCard = findCard('Account Destination');
            if (!tokenCard || !destinationCard || tokenCard.parentElement !== destinationCard.parentElement) return;
            const row = tokenCard.parentElement;
            row.style.display = 'grid';
            row.style.alignItems = 'stretch';
            row.style.gridTemplateColumns = window.innerWidth < 860
                ? 'minmax(0, 1fr)'
                : 'minmax(0, 1fr) minmax(0, 1fr)';
            row.style.gap = window.innerWidth < 640 ? '12px' : '16px';
            tokenCard.style.minWidth = '0';
            destinationCard.style.minWidth = '0';
            [tokenCard, destinationCard].forEach(card => {
                card.style.width = '100%';
                card.style.maxWidth = 'none';
                card.querySelectorAll('input, [role="combobox"]').forEach(control => {
                    control.style.maxWidth = '100%';
                    control.style.minWidth = '0';
                });
                if (window.innerWidth < 640) {
                    card.querySelectorAll('button').forEach(button => {
                        button.style.minWidth = '0';
                        button.style.maxWidth = '100%';
                    });
                    card.querySelectorAll('button').forEach(button => {
                        const group = button.parentElement;
                        const groupButtons = group
                            ? Array.from(group.children).filter(child => child.tagName === 'BUTTON')
                            : [];
                        if (group && groupButtons.length > 1) {
                            group.style.display = 'flex';
                            group.style.flexWrap = 'wrap';
                            group.style.gap = '8px';
                            groupButtons.forEach(child => { child.style.flex = '1 1 120px'; });
                        }
                    });
                } else {
                    card.querySelectorAll('button').forEach(button => {
                        button.style.flex = '';
                        const group = button.parentElement;
                        if (group) {
                            group.style.flexWrap = '';
                            group.style.gap = '';
                        }
                    });
                }
            });
        };

        window.wireVCManagerTokenForm = function() {
            if (
                window.VCManagerTokenFormClickHandler &&
                window.VCManagerTokenFormClickHandler.vcVersion === 'v2'
            ) return;
            if (window.VCManagerTokenFormClickHandler) {
                document.removeEventListener('click', window.VCManagerTokenFormClickHandler, true);
            }

            const dispatchInputEvent = function(input, eventName) {
                let event;
                try {
                    event = new Event(eventName, { bubbles: true, cancelable: false, composed: true });
                } catch (error) {
                    event = document.createEvent('Event');
                    event.initEvent(eventName, true, false);
                }
                input.dispatchEvent(event);
            };

            const clickHandler = function(event) {
                const target = event.target && event.target.closest
                    ? event.target.closest('button')
                    : null;
                if (!target || target.textContent.trim() !== 'Add Account') return;
                const card = target.closest('.card') || target.parentElement;
                if (!card || !Array.from(card.querySelectorAll('*')).some(element =>
                    element.childElementCount === 0 && element.textContent.trim() === 'Multi-Token Accounts'
                )) return;

                const addButton = target;
                if (addButton.dataset.vcTokenFormReplay === 'true') {
                    delete addButton.dataset.vcTokenFormReplay;
                    return;
                }

                const inputs = Array.from(card.querySelectorAll('input')).slice(0, 2);
                if (inputs.length === 0) return;

                // Mobile browsers can deliver the button click before the final
                // controlled-input update. Send input/change first, then replay
                // the click after Nighty's callback queue has received them.
                event.preventDefault();
                event.stopPropagation();
                if (typeof event.stopImmediatePropagation === 'function') {
                    event.stopImmediatePropagation();
                }
                inputs.forEach(input => {
                    dispatchInputEvent(input, 'input');
                    dispatchInputEvent(input, 'change');
                });
                addButton.dataset.vcTokenFormReplay = 'true';
                setTimeout(function() {
                    if (addButton.isConnected && !addButton.disabled) addButton.click();
                    else delete addButton.dataset.vcTokenFormReplay;
                }, 120);
            };
            clickHandler.vcVersion = 'v2';
            window.VCManagerTokenFormClickHandler = clickHandler;
            document.addEventListener('click', clickHandler, true);
        };
        window.wireVCManagerTokenForm();

        window.sizeVCParticipantCard = function() {
            const findCard = prefix => {
                const title = Array.from(document.querySelectorAll('*')).find(element =>
                    element.childElementCount === 0 && element.textContent.trim().startsWith(prefix)
                );
                return title ? (title.closest('.card') || title.parentElement) : null;
            };
            const participantCard = findCard('Channel Participants');
            const automationCard = findCard('Automation & Timers');
            const multiTokenCard = findCard('Multi-Token Accounts');
            if (!participantCard || !automationCard) return;
            participantCard.style.position = 'relative';

            if (window.innerWidth < 900) {
                participantCard.style.height = '';
                participantCard.style.minHeight = '';
                participantCard.style.maxHeight = '';
                participantCard.style.overflow = '';
                return;
            }

            participantCard.style.height = '';
            participantCard.style.minHeight = '';
            participantCard.style.maxHeight = '';
            const participantTop = participantCard.getBoundingClientRect().top;
            const automationBottom = automationCard.getBoundingClientRect().bottom;
            const multiTokenTop = multiTokenCard ? multiTokenCard.getBoundingClientRect().top : automationBottom + 16;
            const targetBottom = Math.max(automationBottom, multiTokenTop - 16);
            const targetHeight = Math.max(390, Math.round(targetBottom - participantTop));
            participantCard.style.height = `${targetHeight}px`;
            participantCard.style.minHeight = `${targetHeight}px`;
            participantCard.style.maxHeight = `${targetHeight}px`;
            participantCard.style.overflow = 'hidden';
        };

        window.styleParticipantIds = function() {
            const title = Array.from(document.querySelectorAll('*')).find(element =>
                element.childElementCount === 0 &&
                element.textContent.trim().startsWith('Channel Participants')
            );
            const card = title ? (title.closest('.card') || title.parentElement) : null;
            if (!card) return;

            card.querySelectorAll('*').forEach(element => {
                if (element.childElementCount === 0 && /^ID:\\s*\\d+$/.test(element.textContent.trim())) {
                    element.style.fontSize = '9px';
                    element.style.lineHeight = '1.1';
                    element.style.opacity = '0.58';
                    element.style.letterSpacing = '0.01em';
                }
            });
        };

        window.positionVCManagerTablePagers = function() {
            ['Channel Participants', 'Managed Accounts'].forEach(titlePrefix => {
                const title = Array.from(document.querySelectorAll('*')).find(element =>
                    element.childElementCount === 0 &&
                    element.textContent.trim().startsWith(titlePrefix)
                );
                const card = title ? (title.closest('.card') || title.parentElement) : null;
                if (!card) return;

                const counters = Array.from(card.querySelectorAll('*')).filter(element =>
                    element.childElementCount === 0 &&
                    /^(?:page\\s*)?\\d+\\s*(?:\\/|of)\\s*\\d+$/i.test(element.textContent.trim())
                );
                counters.forEach(counter => {
                    const controls = counter.parentElement;
                    if (!controls || controls.querySelectorAll('button').length === 0) return;
                    card.style.position = 'relative';
                    controls.style.position = 'absolute';
                    controls.style.top = '16px';
                    controls.style.right = '16px';
                    controls.style.zIndex = '10';
                    controls.style.margin = '0';
                });
            });
        };

        window.VCManagerRefreshLayout = function() {
            window.styleParticipantIds();
            window.positionVCManagerTablePagers();
            window.sizeVCParticipantCard();
            window.sizeVCManagedAccountsCard();
            window.sizeVCAccountSetupCards();
            window.wireVCManagerTokenForm();
            document.querySelectorAll('.vc-managed-accounts-pager').forEach(root => {
                if (root.dataset.managedPagerReady !== 'true') {
                    window.VCManagedAccountsPager.render(root.id);
                }
            });
        };

        window.VCManagerScheduleLayout = function() {
            if (window.VCManagerLayoutPending) return;
            window.VCManagerLayoutPending = true;
            let finished = false;
            const run = function() {
                if (finished) return;
                finished = true;
                window.VCManagerLayoutPending = false;
                window.VCManagerRefreshLayout();
            };
            if (typeof window.requestAnimationFrame === 'function' && document.visibilityState !== 'hidden') {
                window.requestAnimationFrame(run);
                setTimeout(run, 160);
            } else {
                setTimeout(run, 0);
            }
        };

        if (window.VCManagerLayoutObserver && typeof window.VCManagerLayoutObserver.disconnect === 'function') {
            window.VCManagerLayoutObserver.disconnect();
        }
        if (window.VCManagerLayoutTimer) clearInterval(window.VCManagerLayoutTimer);
        if (typeof MutationObserver === 'function' && (document.documentElement || document.body)) {
            window.VCManagerLayoutObserver = new MutationObserver(window.VCManagerScheduleLayout);
            window.VCManagerLayoutObserver.observe(document.documentElement || document.body, { childList: true, subtree: true });
        } else {
            window.VCManagerLayoutTimer = setInterval(window.VCManagerScheduleLayout, 1000);
        }

        setTimeout(() => {
            window.styleParticipantIds();
            window.positionVCManagerTablePagers();
            window.sizeVCParticipantCard();
            window.sizeVCManagedAccountsCard();
            window.sizeVCAccountSetupCards();
            window.wireVCManagerTokenForm();
            Object.keys(window.VCParticipantPager.states).forEach(pagerId =>
                window.VCParticipantPager.render(pagerId)
            );
            Object.keys(window.VCManagedAccountsPager.states).forEach(pagerId =>
                window.VCManagedAccountsPager.render(pagerId)
            );
        }, 50);

        if (window.VCManagerResizeHandler) window.removeEventListener('resize', window.VCManagerResizeHandler);
        window.VCManagerResizeHandler = () => {
            window.VCManagerScheduleLayout();
        };
        window.addEventListener('resize', window.VCManagerResizeHandler);
    })();
    """

    @cui_new(type="callable", name="ChannelInfoModal")
    class ChannelInfoModal:
        _init_params = ["self", "channel_name", "guild_name", "channel_id", "bitrate", "user_limit", "rtc_region", "participants_count", "onClose"]

        def __init__(self, channel_name="", guild_name="", channel_id="", bitrate="64 kbps", user_limit="Unlimited", rtc_region="Automatic", participants_count="0", onClose=None):
            self.channel_name = channel_name
            self.guild_name = guild_name
            self.channel_id = channel_id
            self.bitrate = bitrate
            self.user_limit = user_limit
            self.rtc_region = rtc_region
            self.participants_count = participants_count
            self.onClose = onClose

            self.html = """
            <div style="position: fixed; inset: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.75); backdrop-filter: blur(5px); z-index: 999999; display: flex; align-items: center; justify-content: center;">
            <div class="modal-card" style="
                background: #000000;
                border: 1px solid rgba(255, 255, 255, 0.16);
                border-radius: 16px;
                padding: 24px;
                width: 440px;
                max-width: 90vw;
                box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.7);
                font-family: inherit;
            ">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.08); padding-bottom: 12px;">
                    <span style="font-size: 16px; font-weight: 700;">Channel Telemetry</span>
                    <button onclick="${onClose}" style="
                        background: rgba(255,255,255,0.06);
                        border: 1px solid rgba(255,255,255,0.1);
                        border-radius: 8px;
                        width: 28px;
                        height: 28px;
                        cursor: pointer;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        font-size: 14px;
                    ">✕</button>
                </div>

                <div style="display: flex; flex-direction: column; gap: 12px; margin-bottom: 24px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">Channel Name</span>
                        <span style="font-weight: 600; font-size: 13px;">#${data.channel_name}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">Server (Guild)</span>
                        <span style="font-weight: 600; font-size: 13px;">${data.guild_name}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">Channel ID</span>
                        <span style="font-family: monospace; font-size: 12px;">${data.channel_id}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">Audio Bitrate</span>
                        <span style="font-weight: 600; font-size: 13px;">${data.bitrate}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">User Limit</span>
                        <span style="font-weight: 600; font-size: 13px;">${data.user_limit}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">RTC Region</span>
                        <span style="font-weight: 600; font-size: 13px;">${data.rtc_region}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                        <span style="font-size: 13px;">Active Occupancy</span>
                        <span style="font-weight: 600; font-size: 13px;">${data.participants_count} users</span>
                    </div>
                </div>

                <div style="display: flex; justify-content: flex-end;">
                    <button onclick="${onClose}" style="
                        background: #4f46e5;
                        border: none;
                        padding: 8px 18px;
                        border-radius: 8px;
                        font-weight: 600;
                        font-size: 13px;
                        cursor: pointer;
                        box-shadow: 0 4px 12px rgba(79, 70, 229, 0.4);
                    ">Close</button>
                </div>
            </div>
            </div>
            """

        def create_js(self):
            return "element.style.opacity = '0'; element.style.transition = 'opacity 0.15s ease-out'; requestAnimationFrame(() => element.style.opacity = '1');"

        def remove_js(self):
            return "element.style.opacity = '0';"

    @cui_new(type="callable", name="SessionHistoryModal")
    class SessionHistoryModal:
        _init_params = ["self", "title", "sessions_json", "total_vc_time", "onClear", "onClose"]

        def __init__(self, title="Voice Session History", sessions_json="[]", total_vc_time="0s", onClear=None, onClose=None):
            self.title = title
            self.sessions_json = sessions_json
            self.total_vc_time = total_vc_time
            self.onClear = onClear
            self.onClose = onClose

            self.html = """
            <div style="position: fixed; inset: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.75); backdrop-filter: blur(5px); z-index: 999999; display: flex; align-items: center; justify-content: center;">
            <div class="modal-card" style="
                background: #000000;
                border: 1px solid rgba(255, 255, 255, 0.16);
                border-radius: 16px;
                padding: 24px;
                width: 520px;
                max-width: 90vw;
                box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.7);
                font-family: inherit;
            ">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; border-bottom: 1px solid rgba(255,255,255,0.08); padding-bottom: 12px;">
                    <span style="font-size: 16px; font-weight: 700;">${data.title}</span>
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span style="font-size: 12px;">Total: <strong>${data.total_vc_time}</strong></span>
                        <button onclick="${onClose}" style="
                            background: rgba(255,255,255,0.06);
                            border: 1px solid rgba(255,255,255,0.1);
                            border-radius: 8px;
                            width: 28px;
                            height: 28px;
                            cursor: pointer;
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            font-size: 14px;
                        ">✕</button>
                    </div>
                </div>

                <div id="cui_session_history_list" style="
                    max-height: 280px;
                    overflow-y: auto;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    margin-bottom: 20px;
                    padding-right: 4px;
                ">
                    <!-- Session rows inserted dynamically -->
                </div>

                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <button onclick="${onClear}" style="
                        background: rgba(239, 68, 68, 0.15);
                        border: 1px solid rgba(239, 68, 68, 0.3);
                        padding: 8px 14px;
                        border-radius: 8px;
                        font-weight: 600;
                        font-size: 12px;
                        cursor: pointer;
                    ">Clear Logs</button>
                    <button onclick="${onClose}" style="
                        background: #4f46e5;
                        border: none;
                        padding: 8px 18px;
                        border-radius: 8px;
                        font-weight: 600;
                        font-size: 13px;
                        cursor: pointer;
                    ">Close</button>
                </div>
            </div>
            </div>
            """

        def create_js(self):
            return """
            try {
                const list = element.querySelector('#cui_session_history_list');
                const sessions = JSON.parse(${data.sessions_json});
                const escapeHtml = (value) => String(value).replace(/[&<>"']/g, (char) => ({
                    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
                })[char]);
                if (list) {
                    list.innerHTML = '';
                    if (!sessions || sessions.length === 0) {
                        list.innerHTML = '<div style="text-align: center; padding: 24px; font-size: 13px;">No voice sessions</div>';
                    } else {
                        sessions.slice().reverse().slice(0, 30).forEach(s => {
                            const row = document.createElement('div');
                            row.style.cssText = 'display: flex; justify-content: space-between; align-items: center; padding: 10px 12px; background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.05); border-radius: 8px;';
                            const cName = s.channel_name || 'Channel';
                            const gName = s.guild_name || 'Server';
                            const durationSeconds = Math.max(0, Math.round(Number(s.duration_seconds) || 0));
                            const hours = Math.floor(durationSeconds / 3600);
                            const minutes = Math.floor((durationSeconds % 3600) / 60);
                            const seconds = durationSeconds % 60;
                            const dur = hours > 0
                                ? `${hours}h ${minutes}m ${seconds}s`
                                : (minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`);
                            const formatTime = value => {
                                if (!value) return 'Unknown';
                                const parsed = new Date(value);
                                return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString();
                            };
                            const joined = formatTime(s.start_time);
                            const endedValue = s.end_time || s.timestamp;
                            const left = endedValue ? formatTime(endedValue) : 'Still connected';
                            row.style.alignItems = 'flex-start';
                            row.innerHTML = '<div style="min-width:0;"><div style="font-weight:600;font-size:13px;">#' + escapeHtml(cName) + '</div><div style="font-size:11px;margin-top:2px;">' + escapeHtml(gName) + '</div><div style="font-size:11px;margin-top:6px;line-height:1.45;"><strong>Joined:</strong> ' + escapeHtml(joined) + '<br><strong>Left:</strong> ' + escapeHtml(left) + '</div></div><div style="flex:0 0 auto;background:rgba(255,255,255,0.06);font-weight:700;font-size:12px;padding:3px 8px;border-radius:6px;">' + escapeHtml(dur) + '</div>';
                            list.appendChild(row);
                        });
                    }
                }
            } catch(e) { console.error('SessionHistoryModal populate error:', e); }
            element.style.opacity = '0';
            element.style.transition = 'opacity 0.15s ease-out';
            requestAnimationFrame(() => element.style.opacity = '1');
            """

        def remove_js(self):
            return "element.style.opacity = '0';"

    @cui_new(type="callable", name="UpdateAvailableModal")
    class UpdateAvailableModal:
        _init_params = ["self", "current_version", "latest_version", "project_url"]

        def __init__(self, current_version=None, latest_version=None, project_url=None):
            self.html = """
                <div style="position:fixed;inset:0;width:100vw;height:100vh;z-index:999999;display:flex;align-items:center;justify-content:center;padding:14px;background:rgba(0,0,0,.72);backdrop-filter:blur(5px);">
                <div style="width:min(430px,calc(100vw - 28px));padding:22px;background:#101214;border:1px solid rgba(255,255,255,.13);border-radius:12px;box-shadow:0 24px 70px rgba(0,0,0,.55);">
                    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:18px;">
                        <div>
                            <div style="font-size:18px;font-weight:700;letter-spacing:-.02em;">VC Manager update available</div>
                            <div style="margin-top:6px;font-size:13px;line-height:1.45;opacity:.74;">Your script is v${data.current_version}. The latest version is v${data.latest_version}.</div>
                        </div>
                        <button type="button" data-cui-close aria-label="Close update notice" style="width:30px;height:30px;border:1px solid rgba(255,255,255,.14);border-radius:8px;background:rgba(255,255,255,.05);cursor:pointer;font-size:18px;">&times;</button>
                    </div>
                    <div style="margin-top:16px;padding:12px 14px;border-radius:9px;background:rgba(255,255,255,.045);font-size:13px;line-height:1.45;">Open the repository, copy the latest <strong>VCManager.py</strong>, and replace the current Nighty script.</div>
                    <div style="display:flex;justify-content:flex-end;gap:8px;margin-top:18px;">
                        <button type="button" data-cui-close style="padding:9px 13px;border:1px solid rgba(255,255,255,.14);border-radius:8px;background:transparent;cursor:pointer;font-weight:600;">Later</button>
                        <a href="${data.project_url}" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;padding:9px 13px;border-radius:8px;background:#4a6d88;text-decoration:none;font-weight:700;">Open repository</a>
                    </div>
                </div>
                </div>
            """

        def create_js(self):
            return """
            element.querySelectorAll('[data-cui-close]').forEach(button => {
                button.addEventListener('click', () => element.remove());
            });
            """

        def remove_js(self):
            return "element.style.opacity = '0';"

    # ==================== DATA PERSISTENCE ====================

    VC_DATA_PATH = Path(getScriptsPath()) / "json" / "vc_manager_data.json"
    TOKENS_DATA_PATH = Path(getScriptsPath()) / "json" / "vc_manager_tokens.json"

    VC_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKENS_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

    file_lock = getattr(bot, "_vc_manager_file_lock", None)
    if file_lock is None:
        file_lock = threading.RLock()
        bot._vc_manager_file_lock = file_lock

    def fresh_data():
        return {
            "version": 2,
            "total_vc_time_seconds": 0.0,
            "sessions": [],
            "settings": {
                "auto_disconnect_minutes": None,
                "leave_when_alone": False,
                "muted": False,
                "deafened": False,
                "streaming": False,
                "camera_on": False,
                "auto_reconnect": False,
                "auto_follow": False,
                "follow_target_id": "",
                "slash_responses": {
                    "ephemeral": True,
                    "use_theme": True,
                    "detailed_status": True
                }
            }
        }

    def backup_broken_file(path):
        """Preserve an unreadable JSON file once per script generation."""
        path_key = str(path)
        if path_key in runtime_state["backed_up_files"] or not path.exists():
            return None
        runtime_state["backed_up_files"].add(path_key)
        try:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            backup_path = path.with_name(f"{path.name}.corrupt-{timestamp}.backup")
            shutil.copy2(path, backup_path)
            return backup_path
        except Exception as backup_error:
            print(f"[VC Manager] Could not back up broken file {path.name}: {backup_error}", type_="ERROR")
            return None

    def load_data():
        try:
            if VC_DATA_PATH.exists():
                with open(VC_DATA_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        raise ValueError("VC data root must be an object")
                    base = fresh_data()
                    total_seconds = float(data.get("total_vc_time_seconds", 0.0))
                    base["total_vc_time_seconds"] = total_seconds if math.isfinite(total_seconds) and total_seconds >= 0 else 0.0
                    sessions = data.get("sessions", [])
                    base["sessions"] = [item for item in sessions if isinstance(item, dict)][-500:] if isinstance(sessions, list) else []
                    if isinstance(data.get("settings"), dict):
                        stored_settings = dict(data["settings"])
                        stored_slash = stored_settings.pop("slash_responses", None)
                        base["settings"].update(stored_settings)
                        if isinstance(stored_slash, dict):
                            base["settings"]["slash_responses"].update(stored_slash)
                    return base
            return fresh_data()
        except Exception as e:
            print(f"[VC Manager] Error loading data: {e}", type_="ERROR")
            backup_path = backup_broken_file(VC_DATA_PATH)
            if backup_path:
                print(f"[VC Manager] Preserved the broken data file as {backup_path.name}.", type_="INFO")
            return fresh_data()

    def write_json_file(path, data, label):
        """Write JSON safely, including Windows hosts that briefly lock targets."""
        tmp = None
        try:
            with file_lock:
                tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
                with open(tmp, "w", encoding="utf-8", newline="\n") as file:
                    json.dump(data, file, indent=4, ensure_ascii=False)
                    file.flush()
                    os.fsync(file.fileno())

                last_error = None
                for attempt in range(5):
                    try:
                        os.replace(tmp, path)
                        return True
                    except PermissionError as error:
                        last_error = error
                        if path.exists():
                            try:
                                os.chmod(path, 0o600)
                            except OSError:
                                pass
                        time.sleep(0.08 * (attempt + 1))

            # Antivirus software or sync tools may block file replacement while
            # allowing writes. Keep the fallback under the process lock.
                try:
                    with open(path, "w", encoding="utf-8", newline="\n") as file:
                        json.dump(data, file, indent=4, ensure_ascii=False)
                        file.flush()
                        os.fsync(file.fileno())
                    return True
                except Exception as overwrite_error:
                    raise OSError(
                        f"atomic replace failed ({last_error}); direct overwrite failed ({overwrite_error})"
                    ) from overwrite_error
        except Exception as error:
            print(
                f"[VC Manager] Error saving {label}: {error}. "
                "Close any editor or sync tool using the JSON file, then try again.",
                type_="ERROR"
            )
            return False
        finally:
            if tmp and tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass

    def save_data(data):
        return write_json_file(VC_DATA_PATH, data, "VC data")

    def load_tokens():
        try:
            if TOKENS_DATA_PATH.exists():
                with open(TOKENS_DATA_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not isinstance(data, dict) or not isinstance(data.get("tokens", []), list):
                        raise ValueError("Token data must contain a tokens list")
                    return data
            return {"tokens": []}
        except Exception as e:
            print(f"[VC Manager] Error loading tokens: {e}", type_="ERROR")
            backup_path = backup_broken_file(TOKENS_DATA_PATH)
            backup_note = f" A backup was saved as {backup_path.name}." if backup_path else ""
            return {
                "tokens": [],
                "_load_error": (
                    f"VC Manager could not read {TOKENS_DATA_PATH.name}. The file may contain broken JSON or Nighty may not have file access. "
                    f"Fix or rename the file, then reload the script. VC Manager left the file untouched.{backup_note}"
                )
            }

    def save_tokens(data):
        return write_json_file(TOKENS_DATA_PATH, data, "tokens")

    def update_saved_token(token_id, **changes):
        """Update one saved account without exposing its token in logs."""
        token_id = str(token_id or "")
        if not token_id:
            return False
        with file_lock:
            data = load_tokens()
            if data.get("_load_error"):
                return False
            for item in data.get("tokens", []):
                if isinstance(item, dict) and str(item.get("id") or "") == token_id:
                    item.update(changes)
                    return save_tokens(data)
        return False

    def record_token_session(token_id, session):
        """Append one managed-account voice session without exposing its token."""
        token_id = str(token_id or "")
        if not token_id or not isinstance(session, dict):
            return False
        with file_lock:
            data = load_tokens()
            if data.get("_load_error"):
                return False
            for item in data.get("tokens", []):
                if not isinstance(item, dict) or str(item.get("id") or "") != token_id:
                    continue
                sessions = item.get("sessions", [])
                if not isinstance(sessions, list):
                    sessions = []
                sessions.append(dict(session))
                item["sessions"] = [entry for entry in sessions if isinstance(entry, dict)][-100:]
                try:
                    duration = max(0.0, float(session.get("duration_seconds", 0) or 0))
                except (TypeError, ValueError):
                    duration = 0.0
                try:
                    previous_total = max(0.0, float(item.get("total_vc_time_seconds", 0) or 0))
                except (TypeError, ValueError):
                    previous_total = 0.0
                item["total_vc_time_seconds"] = previous_total + duration
                return save_tokens(data)
        return False

    # ==================== SCRIPT STATE ====================

    active_channel = None
    reconnect_target = dict(previous_managed_target) if previous_managed_target else None
    connection_start_time = previous_connection_start_time
    disconnect_task = None
    intentional_disconnect = False
    is_muted = False
    is_deafened = False
    is_streaming = False
    is_camera_on = False
    is_auto_reconnect = False
    is_auto_follow = False
    follow_target_id = ""
    is_leave_when_alone = False
    selected_server_id = None
    selected_channel_id = None
    selected_token_id = None
    selected_token_guild_id = None
    selected_token_channel_id = None
    _updating_dropdowns = False
    ui_refs = {}
    token_form_state = {"token": "", "name": ""}
    status_editor_channel_id = None
    # VC Manager owns connections opened through this script. Ignore Nighty's
    # separate voice session.
    managed_target = previous_managed_target

    # Multi-token state
    token_clients = {}
    runtime_state["token_clients"] = token_clients

    def selected_item_id(value):
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
        if isinstance(value, dict):
            value = value.get("id") or value.get("title")
        return str(value).strip() if value is not None else ""

    def get_guild_icon_url(guild):
        """Return a custom guild icon or a Discord-style monogram fallback."""
        for attribute in ("icon", "display_icon", "icon_url"):
            asset = getattr(guild, attribute, None)
            asset_url = getattr(asset, "url", asset)
            if asset_url:
                value = str(asset_url).strip()
                if value and value.lower() not in ("none", "null"):
                    return value

        guild_name = str(getattr(guild, "name", "Server") or "Server").strip()
        words = [word for word in guild_name.split() if word]
        initials = "".join(word[0] for word in words[:3]).upper() or "S"
        initials = (
            initials.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )
        hue = int(getattr(guild, "id", 0) or 0) % 360
        svg = (
            '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">'
            f'<rect width="64" height="64" rx="18" fill="hsl({hue},32%,34%)"/>'
            f'<text x="32" y="38" text-anchor="middle" font-family="Arial,sans-serif" '
            f'font-size="20" font-weight="700" fill="white">{initials}</text></svg>'
        )
        return "data:image/svg+xml;charset=UTF-8," + quote(svg)

    def get_gateway_guild_icon_url(guild):
        """Return an icon for a guild dictionary returned by Discord's API."""
        guild_id = str(guild.get("id") or "0")
        icon_hash = str(guild.get("icon") or "")
        if icon_hash:
            extension = "gif" if icon_hash.startswith("a_") else "png"
            return f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{extension}?size=64"
        guild_name = str(guild.get("name") or "Server").strip()
        words = [word for word in guild_name.split() if word]
        initials = "".join(word[0] for word in words[:3]).upper() or "S"
        initials = (
            initials.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )
        hue = int(guild_id or 0) % 360
        svg = (
            '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">'
            f'<rect width="64" height="64" rx="18" fill="hsl({hue},32%,34%)"/>'
            f'<text x="32" y="38" text-anchor="middle" font-family="Arial,sans-serif" '
            f'font-size="20" font-weight="700" fill="white">{initials}</text></svg>'
        )
        return "data:image/svg+xml;charset=UTF-8," + quote(svg)

    def as_bool(value):
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on", "checked")
        return bool(value)

    def timer_minutes(value):
        try:
            number = float(value or 0)
            if not math.isfinite(number):
                return 0
            return max(0, min(120, int(number)))
        except (TypeError, ValueError):
            return 0

    def safe_create_task(coro):
        task = None
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        try:
            bot_loop = getattr(bot, "loop", None)
        except Exception:
            bot_loop = None

        try:
            if bot_loop and bot_loop.is_running():
                if running_loop is bot_loop:
                    task = bot_loop.create_task(coro)
                else:
    # Nighty may invoke CUI callbacks from worker threads. Queue their work on
    # the bot loop because worker threads may not have an event loop.
                    task = asyncio.run_coroutine_threadsafe(coro, bot_loop)
            elif running_loop and running_loop.is_running():
                task = running_loop.create_task(coro)
        except Exception as e:
            print(f"[VC Manager] Could not schedule task: {e}", type_="ERROR")

        if task is None:
            if inspect.iscoroutine(coro):
                coro.close()
            print(
                "[VC Manager] Nighty has no running async loop. Wait for Nighty to connect, then reload the script.",
                type_="ERROR"
            )
            return None

        runtime_state["tasks"].add(task)
        if hasattr(task, "add_done_callback"):
            task.add_done_callback(lambda completed: runtime_state["tasks"].discard(completed))
        return task

    # Retire transports from the previous script generation without sending a
    # voice leave. A script reload is not an instruction to disconnect.
    async def retire_previous_client(previous_client):
        try:
            previous_client._closing = True
            for task_name in ("heartbeat_task", "listener_task", "reconnect_task"):
                task = getattr(previous_client, task_name, None)
                if task and hasattr(task, "done") and not task.done():
                    task.cancel()
            previous_ws = getattr(previous_client, "ws", None)
            if previous_ws and not previous_ws.closed:
                await previous_ws.close()
            previous_session = getattr(previous_client, "http_session", None)
            if previous_session and not previous_session.closed:
                await previous_session.close()
        except Exception as error:
            print(f"[VC Manager] Could not retire an old account transport: {error}", type_="ERROR")

    for previous_client in previous_token_clients:
        try:
            safe_create_task(retire_previous_client(previous_client))
        except Exception as e:
            print(f"[VC Manager] Could not close a stale token client: {e}", type_="ERROR")

    # ==================== MULTI-TOKEN CLIENT ====================

    gateway_start_lock = None
    gateway_last_start = 0.0

    async def wait_for_gateway_start_slot():
        """Space IDENTIFY sessions so several saved accounts do not connect at once."""
        nonlocal gateway_start_lock, gateway_last_start
        if gateway_start_lock is None:
            gateway_start_lock = asyncio.Lock()
        await gateway_start_lock.acquire()
        delay = 5.25 - (time.monotonic() - gateway_last_start)
        if delay > 0:
            await asyncio.sleep(delay)
        gateway_last_start = time.monotonic()
        return gateway_start_lock

    class GatewayVoiceClient:
        def __init__(self, token, name="Token", record_id=None):
            self.token = token.strip()
            self.name = name
            self.record_id = str(record_id or "")
            self.ws = None
            self.http_session = None
            self.session_id = None
            self.resume_gateway_url = None
            self.user_id = None
            self.avatar_url = ""
            self.sequence = None
            self.current_guild_id = None
            self.current_channel_id = None
            self.is_in_vc = False
            self.is_muted = False
            self.is_deafened = False
            self.is_streaming = False
            self.is_camera_on = False
            self.listener_task = None
            self.heartbeat_task = None
            self.reconnect_task = None
            self._closing = False
            self._starting = False
            self._reconnect_attempts = 0
            self._retry_delay = None
            self._resume_on_hello = False
            self._heartbeat_acknowledged = True
            self._restore_task = None
            self._terminal_failure = False
            self._unexpected_voice_losses = 0
            self._last_logged_error = ""
            self._last_transport_error = ""
            self.desired_guild_id = None
            self.desired_channel_id = None
            self.desired_muted = False
            self.desired_deafened = False
            self.desired_streaming = False
            self.desired_camera = False
            self.voice_session_started_at = None
            self.voice_session_guild_id = None
            self.voice_session_channel_id = None
            self.guild_cache = []
            self.channel_cache = {}
            self.status = "Starting"
            self.last_error = ""

        def _voice_target_names(self, guild_id, channel_id):
            guild_name = str(guild_id or "Server")
            channel_name = str(channel_id or "Channel")
            for guild in self.guild_cache:
                if str(guild.get("id") or "") == str(guild_id or ""):
                    guild_name = str(guild.get("name") or guild_name)
                    break
            for channel in self.channel_cache.get(str(guild_id or ""), []):
                if str(channel.get("id") or "") == str(channel_id or ""):
                    channel_name = str(channel.get("name") or channel_name)
                    break
            return guild_name, channel_name

        def _begin_voice_session(self, guild_id, channel_id):
            if not guild_id or not channel_id:
                return
            if (
                self.voice_session_started_at
                and self.voice_session_guild_id == str(guild_id)
                and self.voice_session_channel_id == str(channel_id)
            ):
                return
            if self.voice_session_started_at:
                self._finish_voice_session("Moved")
            self.voice_session_started_at = datetime.now()
            self.voice_session_guild_id = str(guild_id)
            self.voice_session_channel_id = str(channel_id)

        def _finish_voice_session(self, reason="Left"):
            if not self.voice_session_started_at:
                return False
            ended_at = datetime.now()
            duration = max(0.0, (ended_at - self.voice_session_started_at).total_seconds())
            guild_id = self.voice_session_guild_id
            channel_id = self.voice_session_channel_id
            guild_name, channel_name = self._voice_target_names(guild_id, channel_id)
            session = {
                "guild_id": guild_id,
                "guild_name": guild_name,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "start_time": self.voice_session_started_at.isoformat(),
                "end_time": ended_at.isoformat(),
                "duration_seconds": duration,
                "reason": str(reason or "Left"),
            }
            self.voice_session_started_at = None
            self.voice_session_guild_id = None
            self.voice_session_channel_id = None
            return record_token_session(self.record_id, session)

        def _refresh_row(self):
            try:
                if ui_refs:
                    update_token_table_rows()
            except (NameError, RuntimeError):
                pass

        def _set_status(self, status, error=""):
            self.status = str(status or "Idle")
            self.last_error = str(error or "")
            if self.last_error and self.last_error != self._last_logged_error:
                print(f"[VC Manager] {self.name}: {self.last_error}", type_="ERROR")
                self._last_logged_error = self.last_error
            elif not self.last_error and self.status in ("Ready", "Connected"):
                self._last_logged_error = ""
                self._last_transport_error = ""
            self._refresh_row()

        @staticmethod
        def _normalise_close_code(close_code):
            """Return aiohttp close enums and integers as a plain integer."""
            value = getattr(close_code, "value", close_code)
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        @staticmethod
        def _new_http_session():
            """Create a session whose lifetime is not capped by an HTTP total timeout."""
            timeout = aiohttp.ClientTimeout(
                total=None,
                sock_connect=20,
                sock_read=None,
            )
            return aiohttp.ClientSession(timeout=timeout)

        def _gateway_close_details(self, close_code):
            close_code = self._normalise_close_code(close_code)
            explanations = {
                1006: (
                    "Gateway reconnecting",
                    "The connection to Discord ended unexpectedly (code 1006). VC Manager will open a fresh session.",
                    True,
                    5,
                ),
                4000: ("Gateway disconnected", "Discord reported an unknown gateway error. VC Manager will retry.", True, None),
                4001: ("Gateway rejected", "Discord rejected a gateway operation. Reload the updated script before trying again.", False, None),
                4002: ("Gateway rejected", "Discord could not decode a gateway payload. Reload the updated script before trying again.", False, None),
                4003: ("Gateway reconnecting", "Discord received voice data before login completed. VC Manager will retry.", True, 8),
                4004: ("Token rejected", "Discord rejected this token. Remove the account, copy a fresh token, and add it again.", False, None),
                4005: ("Gateway reconnecting", "Discord detected a duplicate login attempt. VC Manager will retry after a cooldown.", True, 12),
                4007: ("Gateway reconnecting", "Discord rejected the previous sequence. VC Manager will open a fresh session.", True, 8),
                4008: ("Rate limited", "Discord rate-limited this account's gateway. VC Manager will wait five minutes before retrying.", True, 300),
                4009: ("Gateway reconnecting", "The Discord gateway session timed out. VC Manager will open a fresh session.", True, 8),
                4010: ("Gateway rejected", "Discord rejected the gateway shard configuration. Reload the updated script.", False, None),
                4011: ("Gateway rejected", "Discord requires sharding for this account session, which VC Manager cannot provide.", False, None),
                4013: ("Gateway rejected", "Discord rejected the gateway intents. Reload the updated script.", False, None),
                4014: ("Gateway rejected", "Discord disallowed the requested gateway intents. Reload the updated script.", False, None),
            }
            if close_code in explanations:
                return explanations[close_code]
            code_text = str(close_code) if close_code is not None else "unknown"
            return (
                "Gateway disconnected",
                f"The Discord gateway closed with code {code_text}. VC Manager will retry with a fresh session.",
                True,
                None,
            )

        @staticmethod
        def _api_error_message(status, payload=None, operation="contact Discord"):
            payload = payload if isinstance(payload, dict) else {}
            api_message = str(payload.get("message") or "").strip()
            api_code = payload.get("code")
            if status == 401:
                return (
                    "Discord rejected this token. It may be expired, reset, or copied incorrectly. "
                    "Remove the account and add a fresh token. VC Manager will not retry it automatically."
                )
            if status == 403:
                details = f" Discord said: {api_message}." if api_message else ""
                if api_code in (40002, 40003) or "verify" in api_message.lower():
                    return (
                        "Discord requires email, phone, or account verification before this account can continue. "
                        "Complete the check in Discord, then reconnect the account." + details
                    )
                return (
                    f"This account is not allowed to {operation}. Check its server membership, timeout state, "
                    f"and channel permissions.{details}"
                )
            if status == 429:
                return "Discord rate-limited this account. VC Manager paused requests; wait before trying again."
            if status >= 500:
                return "Discord is temporarily unavailable. VC Manager will retry after a cooldown."
            details = f" Discord said: {api_message}." if api_message else ""
            return f"Discord returned HTTP {status} while trying to {operation}.{details}"

        async def _api_get(self, path, operation, attempts=2):
            if self._terminal_failure:
                return None
            if self.http_session is None or self.http_session.closed:
                self.http_session = self._new_http_session()
            url = f"https://discord.com/api/v10{path}"
            headers = {"Authorization": self.token}
            timeout = aiohttp.ClientTimeout(total=15)
            for attempt in range(max(1, attempts)):
                try:
                    async with self.http_session.get(url, headers=headers, timeout=timeout) as response:
                        try:
                            payload = await response.json(content_type=None)
                        except Exception:
                            payload = None
                        if response.status == 200:
                            return payload
                        error = self._api_error_message(response.status, payload, operation)
                        if response.status == 401:
                            self._terminal_failure = True
                            self._set_status("Token rejected", error)
                            return None
                        if response.status == 429 and attempt + 1 < attempts:
                            retry_after = 2.0
                            if isinstance(payload, dict):
                                try:
                                    retry_after = min(max(float(payload.get("retry_after", 2.0)), 1.0), 15.0)
                                except (TypeError, ValueError):
                                    pass
                            await asyncio.sleep(retry_after)
                            continue
                        self._set_status("Request blocked" if response.status == 403 else "Request failed", error)
                        return None
                except asyncio.TimeoutError:
                    if attempt + 1 >= attempts:
                        self._set_status("Request timed out", f"Discord did not answer while trying to {operation}. Check the connection and try again.")
                except aiohttp.ClientError as error:
                    if attempt + 1 >= attempts:
                        print(f"[VC Manager] {self.name} API transport error: {type(error).__name__}: {error}", type_="ERROR")
                        self._set_status("Network error", f"Nighty could not {operation}. Check the connection, VPN, or firewall.")
                if attempt + 1 < attempts:
                    await asyncio.sleep(2 + attempt * 2)
            return None

        async def fetch_guilds(self, force=False):
            if self.guild_cache and not force:
                return list(self.guild_cache)
            payload = await self._api_get("/users/@me/guilds?with_counts=true", "load this account's servers")
            if not isinstance(payload, list):
                return []
            guilds = []
            for guild in payload:
                if not isinstance(guild, dict) or not guild.get("id"):
                    continue
                guilds.append({
                    "id": str(guild["id"]),
                    "name": str(guild.get("name") or "Unnamed server"),
                    "icon": str(guild.get("icon") or ""),
                })
            guilds.sort(key=lambda item: item["name"].casefold())
            self.guild_cache = guilds
            return list(guilds)

        async def fetch_voice_channels(self, guild_id, force=False):
            guild_id = str(guild_id or "")
            if not guild_id:
                return []
            if guild_id in self.channel_cache and not force:
                return list(self.channel_cache[guild_id])
            payload = await self._api_get(f"/guilds/{guild_id}/channels", "load this server's voice channels")
            if not isinstance(payload, list):
                return []
            channels = []
            for channel in payload:
                if not isinstance(channel, dict) or int(channel.get("type", -1)) not in (2, 13):
                    continue
                channels.append({
                    "id": str(channel.get("id")),
                    "name": str(channel.get("name") or "Voice channel"),
                    "position": int(channel.get("position", 0) or 0),
                    "user_limit": int(channel.get("user_limit", 0) or 0),
                })
            channels.sort(key=lambda item: (item["position"], item["name"].casefold()))
            self.channel_cache[guild_id] = channels
            return list(channels)

        def set_desired_voice(self, guild_id, channel_id, mute=False, deaf=False, stream=False, video=False):
            self.desired_guild_id = str(guild_id) if guild_id else None
            self.desired_channel_id = str(channel_id) if channel_id else None
            self.desired_muted = bool(mute)
            self.desired_deafened = bool(deaf)
            self.desired_streaming = bool(stream)
            self.desired_camera = bool(video)

        def clear_desired_voice(self):
            self.desired_guild_id = None
            self.desired_channel_id = None
            self._unexpected_voice_losses = 0
            if self._restore_task and not self._restore_task.done():
                self._restore_task.cancel()
            self._restore_task = None

        async def _channel_access_error(self, channel_id):
            """Return a plain-language channel access error, or an empty string."""
            try:
                if self.http_session is None or self.http_session.closed:
                    self.http_session = self._new_http_session()
                url = f"https://discord.com/api/v10/channels/{int(channel_id)}"
                headers = {"Authorization": self.token}
                timeout = aiohttp.ClientTimeout(total=10)

                for attempt in range(2):
                    async with self.http_session.get(url, headers=headers, timeout=timeout) as response:
                        if response.status == 200:
                            return ""
                        try:
                            payload = await response.json(content_type=None)
                        except Exception:
                            payload = None
                        if response.status == 401:
                            self._terminal_failure = True
                            return self._api_error_message(401, payload, "open the selected voice channel")
                        if response.status == 403:
                            return self._api_error_message(403, payload, "open the selected voice channel")
                        if response.status == 404:
                            return "This account cannot access the selected channel. Check its server membership and channel permissions."
                        if response.status == 429:
                            retry_after = 1.0
                            try:
                                retry_after = min(max(float(payload.get("retry_after", 1.0)), 0.25), 3.0)
                            except Exception:
                                pass
                            if attempt == 0:
                                await asyncio.sleep(retry_after)
                                continue
                            return "Discord rate-limited this account. VC Manager paused the request. Wait a few minutes, then try again."
                        return self._api_error_message(response.status, payload, "open the selected voice channel")
            except TimeoutError:
                return "Discord did not answer the channel check. Check your connection and try again."
            except Exception as e:
                print(f"[VC Manager] Could not check channel access for {self.name}: {e}", type_="ERROR")
                return "The channel check failed. Open the Nighty console for the technical error."
            return ""

        async def _send_gateway_login(self):
            if self._resume_on_hello and self.session_id and self.sequence is not None:
                await self.ws.send_json({
                    "op": 6,
                    "d": {
                        "token": self.token,
                        "session_id": self.session_id,
                        "seq": self.sequence,
                    },
                })
                return
            await self.ws.send_json({
                "op": 2,
                "d": {
                    "token": self.token,
                    "capabilities": 16381,
                    "properties": {
                        "os": "Windows",
                        "browser": "Discord Client",
                        "device": "desktop",
                    },
                    "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
                },
            })

        async def _restore_desired_voice(self):
            if (
                self._closing
                or not self.desired_guild_id
                or not self.desired_channel_id
                or not self.ws
                or self.ws.closed
            ):
                return False
            if self.is_in_vc and self.current_channel_id == self.desired_channel_id:
                return True
            access_error = await self._channel_access_error(self.desired_channel_id)
            if access_error:
                self._set_status("Cannot rejoin", access_error)
                return False
            self._set_status("Restoring voice")
            try:
                await self.ws.send_json({
                    "op": 4,
                    "d": {
                        "guild_id": self.desired_guild_id,
                        "channel_id": self.desired_channel_id,
                        "self_mute": self.desired_muted,
                        "self_deaf": self.desired_deafened,
                        "self_video": self.desired_camera,
                    },
                })
                if self.desired_streaming:
                    await self.ws.send_json({
                        "op": 18,
                        "d": {
                            "type": "guild",
                            "guild_id": self.desired_guild_id,
                            "channel_id": self.desired_channel_id,
                            "preferred_region": None,
                        },
                    })
                return True
            except Exception as error:
                print(f"[VC Manager] Could not restore voice for {self.name}: {error}", type_="ERROR")
                self._set_status("Rejoin failed", "Discord did not accept the voice restore request. VC Manager will keep retrying with a longer delay until you press Leave.")
                return False

        def _schedule_voice_restore(self, initial_delay=0):
            if self._closing or self._terminal_failure or not self.desired_channel_id:
                return None
            existing = self._restore_task
            if existing and not existing.done():
                return existing
            expected_channel = self.desired_channel_id

            async def restore_until_connected():
                wait_seconds = max(float(initial_delay or 0), 0.0)
                attempts = 0
                while (
                    not self._closing
                    and not self._terminal_failure
                    and self.desired_channel_id == expected_channel
                ):
                    if wait_seconds:
                        await asyncio.sleep(wait_seconds)
                    if self.is_in_vc and self.current_channel_id == expected_channel:
                        return
                    await self._restore_desired_voice()
                    for _ in range(25):
                        if self.is_in_vc and self.current_channel_id == expected_channel:
                            return
                        if self._closing or self.desired_channel_id != expected_channel:
                            return
                        await asyncio.sleep(0.2)
                    attempts += 1
                    wait_seconds = min(15 * (2 ** min(attempts, 4)), 300)
                    self._set_status(
                        "Voice reconnecting",
                        f"Discord has not restored this account yet. VC Manager will try again in {wait_seconds} seconds unless you press Leave.",
                    )

            self._restore_task = safe_create_task(restore_until_connected())
            return self._restore_task

        def _schedule_reconnect(self, delay=None):
            if self._closing or self._terminal_failure:
                return None
            existing = self.reconnect_task
            if existing and not existing.done():
                return existing

            async def reconnect_until_open():
                next_delay = max(float(delay or 0), 0.0)
                while not self._closing and not self._terminal_failure:
                    if next_delay:
                        await asyncio.sleep(next_delay)
                    if self._closing or self._terminal_failure:
                        return
                    if await self.start_gateway():
                        return
                    self._reconnect_attempts += 1
                    base_delay = min(5 * (2 ** min(self._reconnect_attempts - 1, 5)), 300)
                    next_delay = base_delay + random.uniform(0.0, min(5.0, base_delay * 0.15))

            self.reconnect_task = safe_create_task(reconnect_until_open())
            return self.reconnect_task

        async def start_gateway(self):
            if self._terminal_failure:
                return False
            if self.ws and not self.ws.closed:
                return True
            if self._starting:
                for _ in range(150):
                    await asyncio.sleep(0.1)
                    if self.ws and not self.ws.closed:
                        return True
                    if not self._starting:
                        break
                return bool(self.ws and not self.ws.closed)
            self._starting = True
            start_lock = None
            try:
                self._closing = False
                self._set_status("Connecting")
                if self.http_session is None or self.http_session.closed:
                    self.http_session = self._new_http_session()
                self._resume_on_hello = bool(self.session_id and self.sequence is not None)
                start_lock = await wait_for_gateway_start_slot()
            # Discord starts an opcode heartbeat after HELLO. Disable aiohttp's
            # WebSocket ping timer because zero can trigger a timeout at once.
            # Limit the opening handshake with asyncio.wait_for; some aiohttp
            # builds treat ws_connect's numeric timeout as a receive timeout.
                gateway_base = self.resume_gateway_url or "wss://gateway.discord.gg"
                gateway_url = f"{gateway_base.rstrip('/')}/?v=10&encoding=json"
                self.ws = await asyncio.wait_for(
                    self.http_session.ws_connect(
                        gateway_url,
                        heartbeat=None,
                        autoclose=True,
                        autoping=True,
                    ),
                    timeout=20,
                )
                self.listener_task = safe_create_task(self._gateway_loop())
                if self.listener_task is None:
                    self._set_status("Gateway failed", "Nighty could not start the account gateway task. Restart Nighty and try again.")
                    return False
                return True
            except Exception as e:
                print(f"[VC Manager] Gateway connect failed for {self.name}: {e}", type_="ERROR")
                self._set_status("Gateway failed", "Could not reach Discord. Check your connection, VPN, or firewall, then try again.")
                if self.http_session and not self.http_session.closed:
                    await self.http_session.close()
                self.http_session = None
                self.ws = None
                if self.desired_channel_id and not self._closing and not self._terminal_failure:
                    self._schedule_reconnect(15)
                return False
            finally:
                if start_lock and start_lock.locked():
                    start_lock.release()
                self._starting = False

        async def _heartbeat(self, interval):
            try:
                interval_seconds = max(float(interval) / 1000.0, 1.0)
                await asyncio.sleep(random.random() * interval_seconds)
                while self.ws and not self.ws.closed:
                    if not self._heartbeat_acknowledged:
                        self._retry_delay = 5
                        self._set_status(
                            "Reconnecting",
                            "Discord stopped acknowledging this account's gateway heartbeat. VC Manager is opening a fresh connection.",
                        )
                        await self.ws.close(code=4000, message=b"Heartbeat ACK timeout")
                        return
                    self._heartbeat_acknowledged = False
                    await self.ws.send_json({"op": 1, "d": self.sequence})
                    await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                pass
            except Exception as error:
                if not self._closing:
                    print(f"[VC Manager] Heartbeat stopped for {self.name}: {error}", type_="ERROR")

        async def _gateway_loop(self):
            should_reconnect = True
            try:
                async for msg in self.ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get("s") is not None:
                            self.sequence = data["s"]
                        op = data.get("op")
                        t = data.get("t")
                        d = data.get("d")

                        if op == 10:
                            interval = d["heartbeat_interval"]
                            self._heartbeat_acknowledged = True
                            self.heartbeat_task = safe_create_task(self._heartbeat(interval))
                            await self._send_gateway_login()
                        elif op == 1:
                            await self.ws.send_json({"op": 1, "d": self.sequence})
                        elif op == 11:
                            self._heartbeat_acknowledged = True
                        elif t == "READY":
                            self.session_id = d.get("session_id")
                            resume_url = str(d.get("resume_gateway_url") or "").strip()
                            if resume_url:
                                self.resume_gateway_url = resume_url
                            user = d.get("user") or {}
                            self.user_id = str(user.get("id")) if user.get("id") else None
                            avatar_hash = str(user.get("avatar") or "")
                            if self.user_id and avatar_hash:
                                extension = "gif" if avatar_hash.startswith("a_") else "png"
                                self.avatar_url = (
                                    f"https://cdn.discordapp.com/avatars/{self.user_id}/"
                                    f"{avatar_hash}.{extension}?size=64"
                                )
                            elif self.user_id:
                                avatar_index = (int(self.user_id) >> 22) % 6
                                self.avatar_url = f"https://cdn.discordapp.com/embed/avatars/{avatar_index}.png"
                            try:
                                refresh_destination_account_items(preserve_selection=True)
                            except (NameError, RuntimeError):
                                pass
                            self._reconnect_attempts = 0
                            self._set_status("Ready")
                            if self.desired_channel_id:
                                self._schedule_voice_restore()
                        elif t == "RESUMED":
                            self._reconnect_attempts = 0
                            self._set_status("Connected" if self.desired_channel_id else "Ready")
                            if self.desired_channel_id:
                                self._schedule_voice_restore()
                        elif t == "VOICE_STATE_UPDATE":
                            if self.user_id and str(d.get("user_id")) == self.user_id:
                                was_in_vc = self.is_in_vc
                                previous_guild_id = self.current_guild_id
                                previous_channel_id = self.current_channel_id
                                channel_id = d.get("channel_id")
                                guild_id = d.get("guild_id")
                                self.is_in_vc = channel_id is not None
                                self.current_channel_id = str(channel_id) if channel_id else None
                                self.current_guild_id = str(guild_id) if channel_id and guild_id else None
                                if not channel_id:
                                    if was_in_vc or self.voice_session_started_at:
                                        self._finish_voice_session(
                                            "Disconnected" if self.desired_channel_id else "Left"
                                        )
                                    self.is_muted = False
                                    self.is_deafened = False
                                    self.is_streaming = False
                                    self.is_camera_on = False
                                    if self.desired_channel_id and not self._closing:
                                        self._unexpected_voice_losses += 1
                                        delay = min(15 * (2 ** min(self._unexpected_voice_losses - 1, 4)), 300)
                                        self._set_status(
                                            "Voice disconnected",
                                            f"Discord removed this account from voice. VC Manager will retry in {delay} seconds unless you press Leave.",
                                        )
                                        if self._restore_task and not self._restore_task.done():
                                            self._restore_task.cancel()
                                        self._restore_task = None
                                        self._schedule_voice_restore(delay)
                                    else:
                                        self._set_status("Ready")
                                else:
                                    if (
                                        was_in_vc
                                        and previous_channel_id
                                        and str(previous_channel_id) != str(channel_id)
                                    ):
                                        self._finish_voice_session("Moved")
                                    self._begin_voice_session(guild_id or previous_guild_id, channel_id)
                                    self._unexpected_voice_losses = 0
                                    self.is_muted = bool(d.get("self_mute", self.is_muted))
                                    self.is_deafened = bool(d.get("self_deaf", self.is_deafened))
                                    self.is_streaming = bool(d.get("self_stream", self.is_streaming))
                                    self.is_camera_on = bool(d.get("self_video", self.is_camera_on))
                                    self._set_status("Connected")
                        elif op == 7:
                            self._set_status("Reconnecting")
                            await self.ws.close()
                        elif op == 9:
                            can_resume = bool(d)
                            if not can_resume:
                                self.session_id = None
                                self.sequence = None
                                self.resume_gateway_url = None
                                self._resume_on_hello = False
                            self._retry_delay = 8
                            self._set_status("Reconnecting")
                            await self.ws.close()
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                        close_code = getattr(self.ws, "close_code", None)
                        status, message, should_reconnect, retry_delay = self._gateway_close_details(close_code)
                        if not should_reconnect:
                            self._terminal_failure = True
                        self._retry_delay = retry_delay
                        if close_code or not self._closing:
                            self._set_status(status, message)
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        close_code = getattr(self.ws, "close_code", None)
                        close_code_value = self._normalise_close_code(close_code)
                        exception = self.ws.exception() if self.ws else None
                        status, message, should_reconnect, retry_delay = self._gateway_close_details(close_code)
                        if not should_reconnect:
                            self._terminal_failure = True
                        self._retry_delay = retry_delay
                        if exception:
                            exception_text = str(exception).strip() or "no additional details"
                            code_text = close_code_value if close_code_value is not None else "unknown"
                            transport_error = (
                                f"{type(exception).__name__}: {exception_text} "
                                f"(close code {code_text})"
                            )
                            if transport_error != self._last_transport_error:
                                print(
                                    f"[VC Manager] {self.name} gateway transport error: {transport_error}",
                                    type_="ERROR"
                                )
                                self._last_transport_error = transport_error
                        self._set_status(status, message)
                        break
            except asyncio.CancelledError:
                should_reconnect = False
                raise
            except Exception as e:
                print(f"[VC Manager] Gateway loop terminated for {self.name}: {e}", type_="ERROR")
                if not self.last_error:
                    self._set_status("Gateway failed", "The account lost its Discord connection. VC Manager will retry it.")
            finally:
                self.is_in_vc = False
                self.current_channel_id = None
                self.current_guild_id = None
                if self.heartbeat_task and not self.heartbeat_task.done():
                    self.heartbeat_task.cancel()
                if self._restore_task and not self._restore_task.done():
                    self._restore_task.cancel()
                self._restore_task = None
                self._refresh_row()
                if not self._closing and should_reconnect:
                    self._reconnect_attempts += 1
                    delay = self._retry_delay or min(5 * (2 ** min(self._reconnect_attempts - 1, 5)), 300)
                    delay += random.uniform(0.0, min(5.0, delay * 0.15))
                    self._retry_delay = None
                    self._schedule_reconnect(delay)

        async def join_vc(self, guild_id, channel_id, mute=False, deaf=False, stream=False, video=False):
            if not await self.start_gateway():
                return False
            for _ in range(120):
                if self.user_id and self.ws and not self.ws.closed:
                    break
                if self.status == "Token rejected" or (self.ws and self.ws.closed):
                    break
                await asyncio.sleep(0.1)

            if not self.user_id and not self.last_error:
                self._set_status(
                    "Login timed out",
                    "Discord did not finish signing this account in. Check the token, account verification, VPN, and firewall."
                )

            if not self.ws or self.ws.closed or not self.user_id:
                if not self.last_error:
                    self._set_status("Login failed", "The account gateway closed before Discord confirmed the login. Add the account again with a fresh token.")
                return False

            if not any(
                str(guild.get("id") or "") == str(guild_id)
                for guild in self.guild_cache
            ):
                await self.fetch_guilds(force=False)

            if self.is_in_vc and self.current_channel_id == str(channel_id):
                self.set_desired_voice(guild_id, channel_id, mute, deaf, stream, video)
                self._begin_voice_session(guild_id, channel_id)
                self._set_status("Connected")
                state_saved = update_saved_token(
                    self.record_id,
                    preferred_guild_id=str(guild_id),
                    preferred_channel_id=str(channel_id),
                    stay_connected=True,
                    muted=self.is_muted,
                    deafened=self.is_deafened,
                    streaming=self.is_streaming,
                    camera_on=self.is_camera_on,
                )
                if not state_saved:
                    self.last_error = (
                        "Connected, but Nighty could not save this destination. "
                        "Keep Nighty open and check the scripts JSON folder permissions."
                    )
                return True

            access_error = await self._channel_access_error(channel_id)
            if access_error:
                self._set_status("Join blocked", access_error)
                return False

            self.is_muted = bool(mute)
            self.is_deafened = bool(deaf)
            self.is_streaming = bool(stream)
            self.is_camera_on = bool(video)
            self.set_desired_voice(guild_id, channel_id, mute, deaf, stream, video)
            self._set_status("Joining")

            payload = {
                "op": 4,
                "d": {
                    "guild_id": str(guild_id),
                    "channel_id": str(channel_id) if channel_id else None,
                    "self_mute": self.is_muted,
                    "self_deaf": self.is_deafened,
                    "self_video": self.is_camera_on
                }
            }
            try:
                await self.ws.send_json(payload)
            except Exception as e:
                print(f"[VC Manager] Voice join packet failed for {self.name}: {e}", type_="ERROR")
                self._set_status("Join failed", "Could not send the voice join request. Reconnect the account and try again.")
                return False

            # Discord may delay VOICE_STATE_UPDATE when several tokens connect.
            for _ in range(60):
                if self.is_in_vc and self.current_channel_id == str(channel_id):
                    if self.is_streaming:
                        try:
                            await self.ws.send_json({
                                "op": 18,
                                "d": {
                                    "type": "guild",
                                    "guild_id": str(guild_id),
                                    "channel_id": str(channel_id),
                                    "preferred_region": None
                                }
                            })
                        except Exception as e:
                            print(f"[VC Manager] Stream-state request failed for {self.name}: {e}", type_="ERROR")
                    self._set_status("Connected")
                    state_saved = update_saved_token(
                        self.record_id,
                        preferred_guild_id=str(guild_id),
                        preferred_channel_id=str(channel_id),
                        stay_connected=True,
                        muted=self.is_muted,
                        deafened=self.is_deafened,
                        streaming=self.is_streaming,
                        camera_on=self.is_camera_on,
                    )
                    if not state_saved:
                        self.last_error = (
                            "Connected, but Nighty could not save this destination. "
                            "Keep Nighty open and check the scripts JSON folder permissions."
                        )
                    return True
                if not self.ws or self.ws.closed:
                    break
                await asyncio.sleep(0.2)

            access_error = await self._channel_access_error(channel_id)
            self.clear_desired_voice()
            self.current_channel_id = None
            self.current_guild_id = None
            self.is_in_vc = False
            self._set_status(
                "Join failed",
                access_error or (
                    "Discord did not confirm the join. Check that this account is in the server, can view the channel, "
                    "has Connect permission, is not timed out, and does not need phone or email verification."
                )
            )
            return False

        async def set_voice_controls(self, mute=None, deaf=None, stream=None, video=None):
            """Update this managed account without changing its voice channel."""
            if (
                not self.is_in_vc
                or not self.current_guild_id
                or not self.current_channel_id
                or not self.ws
                or self.ws.closed
            ):
                self._set_status("Not connected", "Connect this account before changing its voice controls.")
                return False

            requested_changes = (
                mute is not None and as_bool(mute) != self.is_muted,
                deaf is not None and as_bool(deaf) != self.is_deafened,
                stream is not None and as_bool(stream) != self.is_streaming,
                video is not None and as_bool(video) != self.is_camera_on,
            )
            if not any(requested_changes):
                return True

            old_state = (
                self.is_muted,
                self.is_deafened,
                self.is_streaming,
                self.is_camera_on,
            )
            if mute is not None:
                self.is_muted = as_bool(mute)
                self.desired_muted = self.is_muted
            if deaf is not None:
                self.is_deafened = as_bool(deaf)
                self.desired_deafened = self.is_deafened
            if stream is not None:
                self.is_streaming = as_bool(stream)
                self.desired_streaming = self.is_streaming
            if video is not None:
                self.is_camera_on = as_bool(video)
                self.desired_camera = self.is_camera_on

            try:
                if mute is not None or deaf is not None or video is not None:
                    await self.ws.send_json({
                        "op": 4,
                        "d": {
                            "guild_id": str(self.current_guild_id),
                            "channel_id": str(self.current_channel_id),
                            "self_mute": self.is_muted,
                            "self_deaf": self.is_deafened,
                            "self_video": self.is_camera_on,
                        }
                    })

                if stream is not None:
                    if self.is_streaming:
                        stream_payload = {
                            "op": 18,
                            "d": {
                                "type": "guild",
                                "guild_id": str(self.current_guild_id),
                                "channel_id": str(self.current_channel_id),
                                "preferred_region": None,
                            }
                        }
                    else:
                        stream_payload = {
                            "op": 22,
                            "d": {
                                "stream_key": (
                                    f"guild:{self.current_guild_id}:"
                                    f"{self.current_channel_id}:{self.user_id}"
                                )
                            }
                        }
                    await self.ws.send_json(stream_payload)
            except Exception as e:
                (
                    self.is_muted,
                    self.is_deafened,
                    self.is_streaming,
                    self.is_camera_on,
                ) = old_state
                print(f"[VC Manager] Voice control update failed for {self.name}: {e}", type_="ERROR")
                self._set_status("Control failed", "Discord rejected this account's voice control update.")
                return False

            self._set_status("Connected")
            update_saved_token(
                self.record_id,
                muted=self.is_muted,
                deafened=self.is_deafened,
                streaming=self.is_streaming,
                camera_on=self.is_camera_on,
            )
            return True

        async def leave_vc(self):
            self.clear_desired_voice()
            update_saved_token(self.record_id, stay_connected=False)
            if not self.current_guild_id or not self.ws or self.ws.closed:
                if self.user_id:
                    self._set_status("Ready")
                return False
            try:
                await self.ws.send_json({
                    "op": 4,
                    "d": {
                        "guild_id": str(self.current_guild_id),
                        "channel_id": None,
                        "self_mute": False,
                        "self_deaf": False,
                        "self_video": False
                    }
                })
            except Exception as e:
                print(f"[VC Manager] Voice leave packet failed for {self.name}: {e}", type_="ERROR")
                self._set_status("Leave failed", "Could not send the disconnect request. Restart Nighty if the account still appears connected.")
                return False
            self._finish_voice_session("Left")
            self.is_in_vc = False
            self.current_channel_id = None
            self.current_guild_id = None
            self._set_status("Ready")
            return True

        async def close(self, preserve_voice=False):
            self._closing = True
            if not preserve_voice and self.current_guild_id and self.ws and not self.ws.closed:
                await self.leave_vc()
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
            if self.listener_task:
                self.listener_task.cancel()
            if self.reconnect_task:
                self.reconnect_task.cancel()
            if self._restore_task:
                self._restore_task.cancel()
            if self.ws and not self.ws.closed:
                await self.ws.close()
            if self.http_session and not self.http_session.closed:
                await self.http_session.close()
            self.http_session = None
            self.status = "Closed"

    def init_token_clients():
        data = load_tokens()
        if data.get("_load_error"):
            print(f"[VC Manager] {data['_load_error']}", type_="ERROR")
            return
        current_ids = set()
        data_changed = False
        for idx, item in enumerate(data.get("tokens", [])):
            if not isinstance(item, dict):
                continue
            tok_id = str(item.get("id") or uuid.uuid4())
            if str(item.get("id") or "") != tok_id:
                data_changed = True
            item["id"] = tok_id
            token_str = str(item.get("token", "")).strip()
            name_str = str(item.get("label") or item.get("name") or f"Token #{idx+1}")
            current_ids.add(tok_id)

            existing = token_clients.get(tok_id)
            if existing and existing.token != token_str:
                safe_create_task(existing.close())
                del token_clients[tok_id]
                existing = None
            if not existing and token_str:
                client = GatewayVoiceClient(token_str, name=name_str, record_id=tok_id)
                token_clients[tok_id] = client
                previous_voice = previous_token_states.get(tok_id, {})
                should_restore = as_bool(item.get("stay_connected", False)) or bool(previous_voice)
                if should_restore:
                    preferred_guild = str(previous_voice.get("guild_id") or item.get("preferred_guild_id") or "")
                    preferred_channel = str(previous_voice.get("channel_id") or item.get("preferred_channel_id") or "")
                    if preferred_guild and preferred_channel:
                        client.set_desired_voice(
                            preferred_guild,
                            preferred_channel,
                            previous_voice.get("muted", item.get("muted", False)),
                            previous_voice.get("deafened", item.get("deafened", False)),
                            previous_voice.get("streaming", item.get("streaming", False)),
                            previous_voice.get("camera_on", item.get("camera_on", False)),
                        )
                        if isinstance(previous_voice.get("session_started_at"), datetime):
                            client.voice_session_started_at = previous_voice["session_started_at"]
                            client.voice_session_guild_id = str(previous_voice.get("session_guild_id") or preferred_guild)
                            client.voice_session_channel_id = str(previous_voice.get("session_channel_id") or preferred_channel)
                        item.update({
                            "preferred_guild_id": preferred_guild,
                            "preferred_channel_id": preferred_channel,
                            "stay_connected": True,
                        })
                        data_changed = True
                safe_create_task(client.start_gateway())
            elif existing:
                existing.name = name_str
                existing.record_id = tok_id

        # Cleanup removed tokens
        for tid in list(token_clients.keys()):
            if tid not in current_ids:
                safe_create_task(token_clients[tid].close())
                del token_clients[tid]
        if data_changed:
            save_tokens(data)

        if not save_tokens(data):
            print("[VC Manager] Managed account IDs could not be saved. Existing token data was not replaced.", type_="ERROR")

    # ==================== VOICE STATE HELPERS ====================

    def set_managed_target(guild_id=None, channel_id=None, channel_name=None):
        """Set or clear the voice session owned by VC Manager."""
        nonlocal managed_target
        if guild_id is None or channel_id is None:
            managed_target = None
        else:
            managed_target = {
                "guild_id": str(guild_id),
                "channel_id": str(channel_id),
                "channel_name": str(channel_name or "Voice Channel")
            }
        runtime_state["managed_target"] = managed_target

    def set_connection_start(value):
        """Keep the live session clock available across Nighty hot reloads."""
        nonlocal connection_start_time
        connection_start_time = value
        runtime_state["connection_start_time"] = value

    def get_current_voice_channel():
        """Return only the channel explicitly connected by VC Manager."""
        nonlocal active_channel
        if not managed_target:
            active_channel = None
            return None

        bot_user = getattr(bot, "user", None)
        if not bot_user:
            active_channel = None
            return None

        target_guild_id = str(managed_target.get("guild_id") or "")
        target_channel_id = str(managed_target.get("channel_id") or "")
        if not target_guild_id or not target_channel_id:
            active_channel = None
            return None

        try:
            guild = bot.get_guild(int(target_guild_id))
            if guild:
                channel = guild.get_channel(int(target_channel_id))
                voice_states = getattr(guild, "_voice_states", {})
                state = voice_states.get(bot_user.id) or voice_states.get(str(bot_user.id))
                state_channel = getattr(state, "channel", None) if state else None
                state_channel_id = getattr(state_channel, "id", None)
                if state_channel_id is None and state:
                    state_channel_id = getattr(state, "channel_id", None)

                if channel and str(state_channel_id or "") == target_channel_id:
                    active_channel = channel
                    return channel

                me = getattr(guild, "me", None) or guild.get_member(bot_user.id)
                voice_state = getattr(me, "voice", None) if me else None
                member_channel = getattr(voice_state, "channel", None) if voice_state else None
                if channel and member_channel and str(member_channel.id) == target_channel_id:
                    active_channel = channel
                    return channel
        except Exception:
            pass

        for voice_client in getattr(bot, "voice_clients", []):
            channel = getattr(voice_client, "channel", None)
            guild = getattr(channel, "guild", None) if channel else None
            if (
                channel
                and guild
                and str(guild.id) == target_guild_id
                and str(channel.id) == target_channel_id
                and getattr(voice_client, "is_connected", lambda: True)()
            ):
                active_channel = channel
                return channel

        active_channel = None
        return None

    def is_actually_connected():
        return get_current_voice_channel() is not None

    def can_set_voice_channel_status(channel):
        """Check the channel-specific Voice Channel Status permission."""
        if not channel:
            return False
        try:
            guild = getattr(channel, "guild", None)
            bot_user = getattr(bot, "user", None)
            member = None
            if guild and bot_user:
                member = getattr(guild, "me", None) or guild.get_member(bot_user.id)
            if not member or not hasattr(channel, "permissions_for"):
                return False

            permissions = channel.permissions_for(member)
            if bool(getattr(permissions, "administrator", False)):
                return True
            if bool(getattr(permissions, "set_voice_channel_status", False)):
                return True

    # Nighty builds without the named permission property can read bit 48.
            permission_value = int(getattr(permissions, "value", 0) or 0)
            return bool(permission_value & (1 << 48))
        except Exception:
            return False

    def get_primary_account_token():
        """Read Nighty's in-memory token without logging or persisting it."""
        candidates = [
            getattr(bot, "http", None),
            getattr(getattr(bot, "_connection", None), "http", None),
            bot,
        ]
        for candidate in candidates:
            if candidate is None:
                continue
            for attribute in ("token", "_token"):
                token = getattr(candidate, attribute, None)
                if token:
                    return str(token).strip()
        return ""

    async def set_voice_channel_status(channel, status):
        """Set or clear the active voice channel status through Discord REST."""
        if not channel or not can_set_voice_channel_status(channel):
            return False, "You do not have permission to change this voice channel status."

        status_value = str(status or "").strip()
        if len(status_value) > 500:
            return False, "Voice channel statuses can contain at most 500 characters."

        token = get_primary_account_token()
        if not token:
            return False, "Nighty's active Discord session token is unavailable."

        url = f"https://discord.com/api/v10/channels/{channel.id}/voice-status"
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "DiscordBot (https://nighty.one, VC Manager)"
        }
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.put(url, headers=headers, json={"status": status_value or None}) as response:
                    if response.status in (200, 204):
                        return True, status_value
                    if response.status == 403:
                        return False, "Discord denied the Voice Channel Status permission."
                    if response.status == 429:
                        return False, "Discord rate-limited the status update. Try again shortly."
                    return False, f"Discord rejected the status update (HTTP {response.status})."
        except Exception as e:
            print(f"[VC Manager] Voice channel status update failed: {e}", type_="ERROR")
            return False, "The voice channel status request failed."

    def format_duration(seconds):
        seconds = int(seconds)
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    def format_session_time(value):
        if not value:
            return "Unknown"
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.strftime("%b %d, %Y %I:%M:%S %p")
        except (TypeError, ValueError):
            return str(value)

    def record_session(channel, started_at):
        if not channel or not started_at:
            return
        ended_at = datetime.now()
        duration = max(0.0, (ended_at - started_at).total_seconds())
        data = load_data()
        data["total_vc_time_seconds"] = float(data.get("total_vc_time_seconds", 0.0)) + duration
        data.setdefault("sessions", []).append({
            "channel_id": str(channel.id),
            "channel_name": getattr(channel, "name", "Voice Channel"),
            "guild_name": getattr(getattr(channel, "guild", None), "name", "Unknown"),
            "start_time": started_at.isoformat(),
            "end_time": ended_at.isoformat(),
            "timestamp": ended_at.isoformat(),
            "duration_seconds": duration,
            "reason": "Left",
        })
        data["sessions"] = data["sessions"][-500:]
        save_data(data)

    def get_primary_session_history(include_live=True):
        sessions = [
            dict(session) for session in load_data().get("sessions", [])
            if isinstance(session, dict)
        ]
        current = get_current_voice_channel()
        if include_live and current and connection_start_time:
            sessions.append({
                "channel_id": str(current.id),
                "channel_name": current.name,
                "guild_name": current.guild.name,
                "start_time": connection_start_time.isoformat(),
                "duration_seconds": max(
                    0.0,
                    (datetime.now() - connection_start_time).total_seconds(),
                ),
                "reason": "Active",
            })
        return sessions

    def get_bot_ws(guild_id=None):
        if hasattr(bot, "_get_websocket"):
            try:
                ws = bot._get_websocket(int(guild_id) if guild_id is not None else None)
                if ws:
                    return ws
            except Exception:
                pass
        ws = getattr(bot, "ws", None)
        if ws:
            return ws
        conn = getattr(bot, "_connection", None)
        if conn:
            if hasattr(conn, "_get_websocket") and guild_id is not None:
                try:
                    return conn._get_websocket(int(guild_id))
                except Exception:
                    pass
            for attr in ("ws", "_ws", "websocket"):
                ws = getattr(conn, attr, None)
                if ws:
                    return ws
        return getattr(bot, "_ws", None)

    async def send_stream_state(guild_id, channel_id, enabled):
        """Start or stop Discord Go Live without rewriting the voice state."""
        try:
            guild_id = str(guild_id).strip() if guild_id else None
            channel_id = str(channel_id).strip() if channel_id else None
            if not guild_id or not channel_id:
                return False

            ws = get_bot_ws(guild_id)
            if not ws:
                return False

            if as_bool(enabled):
                payload = {
                    "op": 18,
                    "d": {
                        "type": "guild",
                        "guild_id": guild_id,
                        "channel_id": channel_id,
                        "preferred_region": None
                    }
                }
            else:
                bot_user = getattr(bot, "user", None)
                user_id = getattr(bot_user, "id", None)
                if not user_id:
                    return False
                payload = {
                    "op": 22,
                    "d": {"stream_key": f"guild:{guild_id}:{channel_id}:{user_id}"}
                }

            if hasattr(ws, "send_as_json"):
                await ws.send_as_json(payload)
            elif hasattr(ws, "send_json"):
                await ws.send_json(payload)
            elif hasattr(ws, "socket") and hasattr(ws.socket, "send_str"):
                await ws.socket.send_str(json.dumps(payload))
            else:
                return False
            return True
        except Exception as e:
            print(f"[VC Manager] Error sending stream state: {e}", type_="ERROR")
            return False

    async def send_voice_state(guild_id, channel_id, update_stream=False):
        nonlocal is_muted, is_deafened, is_streaming, is_camera_on
        requested_stream_state = bool(is_streaming)
        try:
            guild_id = str(guild_id).strip() if guild_id else None
            channel_id = str(channel_id).strip() if channel_id else None
            ws = get_bot_ws(guild_id)
            payload = {
                "op": 4,
                "d": {
                    "guild_id": guild_id,
                    "channel_id": channel_id,
                    "self_mute": bool(is_muted),
                    "self_deaf": bool(is_deafened),
                    "self_video": bool(is_camera_on)
                }
            }
            if ws:
                if hasattr(ws, "send_as_json"):
                    await ws.send_as_json(payload)
                    sent = True
                elif hasattr(ws, "send_json"):
                    await ws.send_json(payload)
                    sent = True
                elif hasattr(ws, "socket") and hasattr(ws.socket, "send_str"):
                    await ws.socket.send_str(json.dumps(payload))
                    sent = True
                elif hasattr(ws, "voice_state"):
                    await ws.voice_state(
                        int(guild_id) if guild_id else 0,
                        int(channel_id) if channel_id else None,
                        bool(is_muted),
                        bool(is_deafened)
                    )
                    sent = True
                else:
                    sent = False
            else:
                sent = False

            if not sent and guild_id:
                guild = bot.get_guild(int(guild_id))
                if guild and hasattr(guild, "change_voice_state"):
                    ch = bot.get_channel(int(channel_id)) if channel_id else None
                    await guild.change_voice_state(
                        channel=ch,
                        self_mute=bool(is_muted),
                        self_deaf=bool(is_deafened)
                    )
                    sent = True

            if not sent:
                return False

            if update_stream and guild_id and channel_id:
                return await send_stream_state(guild_id, channel_id, requested_stream_state)
            return True
        except Exception as e:
            print(f"[VC Manager] Error sending voice state: {e}", type_="ERROR")
            return False

    async def update_voice_state(mute=None, deaf=None, stream=None, video=None):
        nonlocal is_muted, is_deafened, is_streaming, is_camera_on
        cur = get_current_voice_channel()
        if not cur:
            return False

        old_state = (is_muted, is_deafened, is_streaming, is_camera_on)
        if mute is not None:
            is_muted = as_bool(mute)
        if deaf is not None:
            is_deafened = as_bool(deaf)
        if stream is not None:
            is_streaming = as_bool(stream)
        if video is not None:
            is_camera_on = as_bool(video)

        requested_stream_state = as_bool(stream) if stream is not None else None
        if stream is not None and mute is None and deaf is None and video is None:
            res = await send_stream_state(cur.guild.id, cur.id, requested_stream_state)
        else:
            res = await send_voice_state(cur.guild.id, cur.id)
            if res and requested_stream_state is not None:
                res = await send_stream_state(cur.guild.id, cur.id, requested_stream_state)

        if not res:
            is_muted, is_deafened, is_streaming, is_camera_on = old_state
            update_all_ui()
            return False

        # VOICE_STATE_UPDATE may report the old stream value between gateway
        # writes. Keep the requested value until Discord sends the stream event.
        if requested_stream_state is not None:
            is_streaming = requested_stream_state

        data = load_data()
        data["settings"]["muted"] = is_muted
        data["settings"]["deafened"] = is_deafened
        data["settings"]["streaming"] = is_streaming
        data["settings"]["camera_on"] = is_camera_on
        save_data(data)

        await sync_managed_voice_controls(
            mute=mute,
            deaf=deaf,
            stream=stream,
            video=video,
        )
        update_all_ui()
        return True

    async def sync_managed_voice_controls(mute=None, deaf=None, stream=None, video=None):
        """Apply the primary controls to connected managed accounts."""
        clients = [client for client in token_clients.values() if client.is_in_vc]
        if not clients:
            return 0, 0

        results = await asyncio.gather(
            *(
                client.set_voice_controls(
                    mute=mute,
                    deaf=deaf,
                    stream=stream,
                    video=video,
                )
                for client in clients
            ),
            return_exceptions=True,
        )
        updated = 0
        for client, result in zip(clients, results):
            if result is True:
                updated += 1
            elif isinstance(result, Exception):
                print(
                    f"[VC Manager] Could not sync voice controls for {client.name}: {result}",
                    type_="ERROR",
                )
                client._set_status(
                    "Control failed",
                    "Nighty could not update this account. Open the Nighty console for details.",
                )
        update_token_table_rows()
        return updated, len(clients)

    async def connect(guild_id, channel_id):
        nonlocal intentional_disconnect, active_channel
        intentional_disconnect = False
        channel = bot.get_channel(int(channel_id)) if str(channel_id).isdigit() else None
        set_managed_target(guild_id, channel_id, getattr(channel, "name", "Voice Channel"))
        active_channel = channel
        sent = await send_voice_state(guild_id, channel_id)
        if not sent:
            active_channel = None
            set_managed_target()
        return sent

    async def disconnect_from_voice(intentional=True):
        nonlocal intentional_disconnect, active_channel
        intentional_disconnect = intentional
        sent = False
        cached_channel = active_channel
        started_at = connection_start_time
        try:
            current = get_current_voice_channel()
            channel = current or cached_channel
            target_guild_id = str((managed_target or {}).get("guild_id") or "")
            if not target_guild_id:
                return False
        # Clear the clock before yielding to prevent duplicate session records.
            set_connection_start(None)
            sent = await send_voice_state(target_guild_id, None)
            if sent:
                record_session(channel, started_at)
                active_channel = None
                set_managed_target()
                if ui_refs:
                    update_all_ui()
            else:
                set_connection_start(started_at)
        except Exception as e:
            set_connection_start(started_at)
            print(f"[VC Manager] Disconnect failed: {e}", type_="ERROR")
        return sent

    async def sync_voice_state(update_ui=True):
        nonlocal active_channel, reconnect_target
        current = get_current_voice_channel()
        if current:
            active_channel = current
            reconnect_target = {
                "guild_id": str(current.guild.id),
                "channel_id": str(current.id),
                "channel_name": current.name
            }
            if connection_start_time is None:
                set_connection_start(datetime.now())
        else:
            active_channel = None
            set_connection_start(None)

        if update_ui and ui_refs:
            update_all_ui()
        return active_channel

    async def schedule_disconnect(minutes):
        nonlocal disconnect_task
        if disconnect_task and not disconnect_task.done():
            disconnect_task.cancel()
        disconnect_task = None

        if minutes is None or minutes <= 0:
            return
        if not is_actually_connected():
            return

        async def timer():
            try:
                await asyncio.sleep(float(minutes) * 60)
                if is_actually_connected():
                    print("[VC Manager] Auto-disconnect timer expired", type_="INFO")
                    await disconnect_from_voice(intentional=True)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[VC Manager] Auto-disconnect error: {e}", type_="ERROR")

        disconnect_task = safe_create_task(timer())

    # ==================== EVENT LISTENERS ====================

    @bot.listen("on_voice_state_update")
    async def handle_voice_state_update(member, before, after):
        nonlocal active_channel, reconnect_target, intentional_disconnect
        nonlocal is_muted, is_deafened, is_streaming, is_camera_on

        bot_user = getattr(bot, "user", None)
        if not bot_user:
            return

        # 1. BOT SELF STATE CHANGE
        if member.id == bot_user.id:
            old_channel = before.channel if before else None
            new_channel = after.channel if after else None
            event_channel = new_channel or old_channel
            event_guild = getattr(event_channel, "guild", None)
            target_guild_id = str((managed_target or {}).get("guild_id") or "")
            target_channel_id = str((managed_target or {}).get("channel_id") or "")

        # Ignore voice activity started outside VC Manager.
            if not target_guild_id or not event_guild or str(event_guild.id) != target_guild_id:
                return

            # A move to another channel ends the managed session.
            if new_channel and str(new_channel.id) != target_channel_id:
                if old_channel and str(old_channel.id) == target_channel_id:
                    record_session(old_channel, connection_start_time)
                active_channel = None
                set_connection_start(None)
                set_managed_target()
                update_all_ui()
                return

            # Disconnected
            if not new_channel:
                record_session(old_channel or active_channel, connection_start_time)

                active_channel = None
                set_connection_start(None)

                # Auto reconnect check
                if not intentional_disconnect and is_auto_reconnect and reconnect_target:
                    print(f"[VC Manager] Unexpected drop from {reconnect_target['channel_name']}. Reconnecting in 3s...", type_="INFO")
                    async def reconnect_later():
                        await asyncio.sleep(3)
                        if not is_actually_connected() and reconnect_target:
                            await connect(reconnect_target["guild_id"], reconnect_target["channel_id"])
                    safe_create_task(reconnect_later())

                intentional_disconnect = False
                if not is_auto_reconnect:
                    set_managed_target()
                update_all_ui()
                return

            # Connected or switched
            if new_channel:
                previous_controls = (
                    is_muted,
                    is_deafened,
                    is_streaming,
                    is_camera_on,
                )
                if old_channel and old_channel.id != new_channel.id and connection_start_time:
                    record_session(old_channel, connection_start_time)
                    set_connection_start(datetime.now())
                active_channel = new_channel
                reconnect_target = {
                    "guild_id": str(new_channel.guild.id),
                    "channel_id": str(new_channel.id),
                    "channel_name": new_channel.name
                }
                if connection_start_time is None:
                    set_connection_start(datetime.now())
                    saved_minutes = load_data().get("settings", {}).get("auto_disconnect_minutes")
                    if saved_minutes:
                        await schedule_disconnect(saved_minutes)

                is_muted = bool(getattr(after, "self_mute", False) or getattr(after, "mute", False))
                is_deafened = bool(getattr(after, "self_deaf", False) or getattr(after, "deaf", False))
                is_streaming = bool(getattr(after, "self_stream", False) or getattr(after, "streaming", False))
                is_camera_on = bool(getattr(after, "self_video", False))

                current_controls = (
                    is_muted,
                    is_deafened,
                    is_streaming,
                    is_camera_on,
                )
                if current_controls != previous_controls:
                    await sync_managed_voice_controls(
                        mute=is_muted,
                        deaf=is_deafened,
                        stream=is_streaming,
                        video=is_camera_on,
                    )

                update_all_ui()
                return

        # 2. OTHER MEMBER UPDATES
        cur = get_current_voice_channel()

    # Auto-follow can connect a disconnected primary account.
        if is_auto_follow and follow_target_id and str(member.id) == str(follow_target_id):
            target_channel = getattr(after, "channel", None) if after else None
            if target_channel and (not cur or target_channel.id != cur.id):
                print(f"[VC Manager] Auto-following target {member.name} to #{target_channel.name}", type_="INFO")
                await connect(target_channel.guild.id, target_channel.id)
                return

        if not cur:
            return

        # Leave when alone check
        if is_leave_when_alone:
            other_people = [
                m for m in getattr(cur, "members", [])
                if m.id != bot_user.id and not getattr(m, "bot", False)
            ]
            if not other_people:
                print(f"[VC Manager] Alone in #{cur.name}, disconnecting...", type_="INFO")
                await disconnect_from_voice(intentional=True)
                return

        # Update participants table if related to active channel
        if (before and before.channel and before.channel.id == cur.id) or (after and after.channel and after.channel.id == cur.id):
            update_participants_table()

    runtime_state["listeners"].append(("on_voice_state_update", handle_voice_state_update))

    # ==================== NIGHTY SLASH API + COMPONENTS V2 ====================

    # Adapted from lord-m1nx/nighty-slash-api. Nighty injects `discord`,
    # `app_commands`, and `bot_app`, so this script uses those objects.
    try:
        discord_api = discord
    except NameError:
        discord_api = None
    try:
        app_commands_api = app_commands
    except NameError:
        app_commands_api = getattr(discord_api, "app_commands", None) if discord_api else None
    try:
        slash_bot_app = bot_app
    except NameError:
        slash_bot_app = None

    COMPONENTS_V2_FLAG = 1 << 15
    EPHEMERAL_FLAG = 1 << 6

    # Remove legacy VC text commands left in Nighty's registry after a reload.
    for legacy_command_name in (
        "joinvc", "vcload", "leavevc", "vcmute", "vcunmute",
        "vcdeafen", "vcundeafen", "vcstream", "vccamera",
        "vclist", "vchistory", "vchelp"
    ):
        try:
            bot.remove_command(legacy_command_name)
        except Exception:
            pass

    def get_slash_response_settings():
        defaults = {
            "ephemeral": True,
            "use_theme": True,
            "detailed_status": True
        }
        stored = load_data().get("settings", {}).get("slash_responses", {})
        if isinstance(stored, dict):
            for key in defaults:
                if key in stored:
                    defaults[key] = as_bool(stored[key])
        return defaults

    def save_slash_response_settings(settings):
        data = load_data()
        data.setdefault("settings", {})["slash_responses"] = {
            "ephemeral": bool(settings.get("ephemeral", True)),
            "use_theme": bool(settings.get("use_theme", True)),
            "detailed_status": bool(settings.get("detailed_status", True))
        }
        save_data(data)

    def sync_command_settings_ui(settings=None):
        settings = settings or get_slash_response_settings()
        toggle_keys = {
            "command_ephemeral_toggle": "ephemeral",
            "command_theme_toggle": "use_theme",
            "command_detailed_toggle": "detailed_status",
        }
        for ref_name, setting_key in toggle_keys.items():
            toggle = ui_refs.get(ref_name)
            if toggle:
                toggle.checked = bool(settings.get(setting_key, True))

    def slash_theme_layout(content):
        """Build Discord Components V2 using nes's Nighty V2 theme schema."""
        def limited_text(value):
            value = str(value or "")
            return value if len(value) <= 3900 else value[:3897] + "..."

        content = limited_text(content)
        theme_data = {}
        if get_slash_response_settings()["use_theme"]:
            try:
                theme_name = getConfigData().get("theme")
                theme_data = getThemeData(theme_name=theme_name) or {}
            except Exception:
                theme_data = {}

        v2 = theme_data.get("v2") if isinstance(theme_data, dict) else None
        if not isinstance(v2, dict) or not v2.get("enabled", False):
            v2 = {"color": theme_data.get("color", "#5865F2"), "layout": [{"text": "{content}"}]}

        color_value = v2.get("color", theme_data.get("color", "#5865F2"))
        try:
            accent_color = int(str(color_value).lstrip("#"), 16)
        except (TypeError, ValueError):
            accent_color = 0x5865F2

        children = []
        injected = False
        for layout_item in v2.get("layout", []):
            if not isinstance(layout_item, dict):
                continue
            if "text" in layout_item:
                value = str(layout_item.get("text") or "")
                injected = injected or "{content}" in value
                children.append({"type": 10, "content": limited_text(value.replace("{content}", content))})
            elif "section" in layout_item and isinstance(layout_item["section"], dict):
                section = layout_item["section"]
                value = str(section.get("text") or "")
                injected = injected or "{content}" in value
                thumbnail = str(section.get("thumbnail") or "").strip()
                if thumbnail.startswith(("https://", "http://")):
                    children.append({
                        "type": 9,
                        "components": [{"type": 10, "content": limited_text(value.replace("{content}", content))}],
                        "accessory": {"type": 11, "media": {"url": thumbnail}}
                    })
                else:
                    children.append({"type": 10, "content": limited_text(value.replace("{content}", content))})
            elif "image" in layout_item:
                image_url = str(layout_item.get("image") or "").strip()
                if image_url.startswith(("https://", "http://")):
                    children.append({
                        "type": 12,
                        "items": [{"media": {"url": image_url}}]
                    })
            elif "separator" in layout_item:
                spacing = 2 if str(layout_item.get("separator")).lower() == "large" else 1
                children.append({"type": 14, "divider": True, "spacing": spacing})

        if not injected:
            children.insert(0, {"type": 10, "content": content})
        if not children:
            children = [{"type": 10, "content": content}]
        return [{"type": 17, "accent_color": accent_color, "components": children[:39]}]

    class VCSlashContext:
        """Slash API context with native Components V2 response support."""
        def __init__(self, interaction):
            self.interaction = interaction
            self._done = False
            self._ephemeral = get_slash_response_settings()["ephemeral"]

        @property
        def user(self):
            return getattr(self.interaction, "user", None)

        async def _fallback_embed(self, title, description, ephemeral=True):
            content = f"**{title}**\n{description}" if title else description
            content = content.replace("@", "@\u200b")
            response = getattr(self.interaction, "response", None)
            if response and not response.is_done():
                self._done = True
                return await response.send_message(content=content[:2000], ephemeral=ephemeral)
            followup = getattr(self.interaction, "followup", None)
            if followup:
                return await followup.send(content=content[:2000], ephemeral=ephemeral)
            return None

        async def send_v2(self, title, description, ephemeral=None):
            if ephemeral is None:
                ephemeral = get_slash_response_settings()["ephemeral"]
            content = f"## {title}\n{description}" if title else description
            interaction_id = str(getattr(self.interaction, "id", "") or "")
            interaction_token = str(getattr(self.interaction, "token", "") or "")
            if interaction_id and interaction_token and not self._done:
                payload = {
                    "type": 4,
                    "data": {
                        "flags": COMPONENTS_V2_FLAG | (EPHEMERAL_FLAG if ephemeral else 0),
                        "components": slash_theme_layout(content),
                        "allowed_mentions": {"parse": []}
                    }
                }
                url = f"https://discord.com/api/v10/interactions/{interaction_id}/{interaction_token}/callback"
                try:
                    timeout = aiohttp.ClientTimeout(total=10)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.post(url, json=payload) as response:
                            if response.status in (200, 204):
                                self._done = True
                                self._ephemeral = ephemeral
                                return True
                            detail = (await response.text())[:300]
                            print(f"[VC Manager] Components V2 response rejected ({response.status}): {detail}", type_="ERROR")
                except Exception as e:
                    print(f"[VC Manager] Components V2 response failed: {e}", type_="ERROR")
            await self._fallback_embed(title, description, ephemeral=ephemeral)
            self._done = True
            return False

        async def edit_v2(self, title, description):
            content = f"## {title}\n{description}" if title else description
            interaction_token = str(getattr(self.interaction, "token", "") or "")
            application_id = getattr(self.interaction, "application_id", None)
            if not application_id and slash_bot_app:
                application_id = getattr(getattr(slash_bot_app, "user", None), "id", None)
            if not application_id or not interaction_token:
                return False
            url = (
                f"https://discord.com/api/v10/webhooks/{application_id}/{interaction_token}"
                "/messages/@original?with_components=true"
            )
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.patch(
                        url,
                        json={"components": slash_theme_layout(content), "allowed_mentions": {"parse": []}}
                    ) as response:
                        if response.status == 200:
                            return True
                        detail = (await response.text())[:300]
                        print(f"[VC Manager] Components V2 edit rejected ({response.status}): {detail}", type_="ERROR")
            except Exception as e:
                print(f"[VC Manager] Components V2 edit failed: {e}", type_="ERROR")
            return False

        async def error(self, message):
            if self._done:
                return await self.edit_v2("VC Manager", message)
            return await self.send_v2("VC Manager", message)

    class SlashAPI:
        """Focused Nighty Slash API adapter for this script's command group."""
        def __init__(self, label):
            self.label = label
            self.registered = []
            self._shapes = []
            self._tree = getattr(slash_bot_app, "tree", None) if slash_bot_app else None
            self._state_path = VC_DATA_PATH.parent / "slashapi_state.json"
            self._session_synced = False
            if not hasattr(bot, "_nighty_slash_active_instances"):
                bot._nighty_slash_active_instances = {}
            bot._nighty_slash_active_instances[label] = self

        @property
        def available(self):
            return self._tree is not None and app_commands_api is not None and discord_api is not None

        def _wrap(self, func):
            @functools.wraps(func)
            async def wrapper(interaction, *args, **kwargs):
                ctx = VCSlashContext(interaction)
                try:
                    return await func(ctx, *args, **kwargs)
                except Exception as e:
                    print(f"[VC Manager Slash] {func.__name__} failed: {e}", type_="ERROR")
                    if not ctx._done:
                        await ctx.error("The command could not be completed.")

            try:
                signature = inspect.signature(func)
                params = list(signature.parameters.values())
                interaction_type = getattr(discord_api, "Interaction", None)
                wrapper.__signature__ = signature.replace(parameters=[
                    inspect.Parameter(
                        "interaction",
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        annotation=interaction_type
                    )
                ] + params[1:])
            except Exception:
                pass
            return wrapper

        def group(self, name, description):
            if not self.available:
                return None
            try:
                self._tree.remove_command(name)
            except Exception:
                pass
            group_obj = app_commands_api.Group(name=name, description=description[:100])

    # Discord reads context and install decorators from the top-level group.
            allowed_contexts = getattr(app_commands_api, "allowed_contexts", None)
            if callable(allowed_contexts):
                try:
                    group_obj = allowed_contexts(
                        guilds=True,
                        dms=True,
                        private_channels=True
                    )(group_obj)
                except Exception as e:
                    print(f"[VC Manager Slash] Could not enable all command contexts: {e}", type_="ERROR")

            allowed_installs = getattr(app_commands_api, "allowed_installs", None)
            if callable(allowed_installs):
                try:
                    group_obj = allowed_installs(guilds=True, users=True)(group_obj)
                except Exception as e:
                    print(f"[VC Manager Slash] Could not enable user installs: {e}", type_="ERROR")
            else:
                user_install = getattr(app_commands_api, "user_install", None)
                if callable(user_install):
                    try:
                        group_obj = user_install()(group_obj)
                    except Exception as e:
                        print(f"[VC Manager Slash] Could not enable user installs: {e}", type_="ERROR")

            self._tree.add_command(group_obj)
            self.registered.append(name)
            return SlashGroup(self, group_obj)

        def fingerprint(self):
            raw = json.dumps(self._shapes, sort_keys=True, separators=(",", ":"))
            return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

        def autosync(self):
            if not self.available:
                print("[VC Manager Slash] bot_app command tree is unavailable.", type_="ERROR")
                return

            async def runner():
                try:
                    wait_ready = getattr(bot, "wait_until_ready", None)
                    if callable(wait_ready):
                        await wait_ready()
                    await asyncio.sleep(3)
                    state = {}
                    if self._state_path.exists():
                        try:
                            with open(self._state_path, "r", encoding="utf-8") as file:
                                state = json.load(file)
                        except Exception:
                            state = {}
                    current = self.fingerprint()
                    previous = (state.get(self.label) or {}).get("fingerprint")
                    if previous != current or not self._session_synced:
                        synced = await self._tree.sync()
                        self._session_synced = True
                        state[self.label] = {
                            "fingerprint": current,
                            "commands": list(self.registered)
                        }
                        with open(self._state_path, "w", encoding="utf-8") as file:
                            json.dump(state, file, indent=2)
                        print(f"[VC Manager Slash] Synced {len(synced)} command(s).", type_="SUCCESS")
                    else:
                        print("[VC Manager Slash] Slash definitions already synced.", type_="INFO")
                except Exception as e:
                    print(f"[VC Manager Slash] Auto-sync failed: {e}", type_="ERROR")

            safe_create_task(runner())

    class SlashGroup:
        def __init__(self, api, group_obj):
            self.api = api
            self.group = group_obj

        def command(self, name, description, describe=None):
            def decorator(func):
                handler = self.api._wrap(func)
                if describe:
                    try:
                        handler = app_commands_api.describe(**describe)(handler)
                    except Exception as e:
                        print(f"[VC Manager Slash] Option descriptions failed: {e}", type_="ERROR")
                result = self.group.command(name=name, description=description[:100])(handler)
                params = []
                for parameter in list(inspect.signature(func).parameters.values())[1:]:
                    params.append({
                        "name": parameter.name,
                        "annotation": getattr(parameter.annotation, "__name__", str(parameter.annotation)),
                        "required": parameter.default is inspect.Parameter.empty
                    })
                self.api._shapes.append({
                    "parent": self.group.name,
                    "name": name,
                    "description": description,
                    "params": params
                })
                return result
            return decorator

    slash = SlashAPI(label="VCManager")
    vc_group = slash.group("vc", "Control VC Manager voice sessions") if slash.available else None

    async def slash_owner_only(ctx):
        caller_id = getattr(ctx.user, "id", None)
        owner_id = getattr(getattr(bot, "user", None), "id", None)
        if caller_id and owner_id and str(caller_id) == str(owner_id):
            return True
        await ctx.error("Only the Nighty account owner can control VC Manager.")
        return False

    def is_voice_channel(channel):
        if not channel or not getattr(channel, "guild", None):
            return False
        channel_type = str(getattr(channel, "type", "")).lower()
        return "voice" in channel_type or "stage" in channel_type or hasattr(channel, "bitrate")

    def find_voice_channel(query, preferred_guild=None):
        query = str(query or "").strip()
        if query.isdigit():
            channel = bot.get_channel(int(query))
            if is_voice_channel(channel):
                return channel
        guilds = list(getattr(bot, "guilds", []))
        if preferred_guild in guilds:
            guilds.remove(preferred_guild)
            guilds.insert(0, preferred_guild)
        candidates = []
        for guild in guilds:
            for channel in list(getattr(guild, "voice_channels", [])) + list(getattr(guild, "stage_channels", [])):
                channel_name = str(getattr(channel, "name", ""))
                if channel_name.casefold() == query.casefold():
                    return channel
                if query.casefold() in channel_name.casefold():
                    candidates.append(channel)
        return candidates[0] if len(candidates) == 1 else None

    if vc_group:
        @vc_group.command(
            name="join",
            description="Join a voice channel by ID or name",
            describe={"channel": "Voice channel ID or part of its name"}
        )
        async def slash_vc_join(ctx, channel: str):
            if not await slash_owner_only(ctx):
                return
            target = find_voice_channel(channel, getattr(ctx.interaction, "guild", None))
            if not target or not getattr(target, "guild", None):
                return await ctx.error(f"Could not find voice channel `{channel}`.")
            await ctx.send_v2("VC Manager", f"Connecting to **#{target.name}** in **{target.guild.name}**...")
            if await connect(target.guild.id, target.id):
                await ctx.edit_v2("Connected", f"VC Manager joined **#{target.name}** in **{target.guild.name}**.")
            else:
                await ctx.edit_v2("Connection Failed", "Discord rejected the voice join request.")

        @vc_group.command(
            name="load",
            description="Join and enable mute, deafen, stream, and camera",
            describe={"channel_id": "Numeric voice channel ID"}
        )
        async def slash_vc_load(ctx, channel_id: str):
            nonlocal intentional_disconnect, is_muted, is_deafened, is_streaming, is_camera_on
            if not await slash_owner_only(ctx):
                return
            channel_id = str(channel_id or "").strip()
            target = find_voice_channel(channel_id) if channel_id.isdigit() else None
            guild = getattr(target, "guild", None)
            if not target or not guild:
                return await ctx.error(f"Could not find voice channel `{channel_id}`.")

            await ctx.send_v2("VC Manager", f"Loading **#{target.name}** with all voice controls...")
            old_state = (is_muted, is_deafened, is_streaming, is_camera_on)
            is_muted = is_deafened = is_streaming = is_camera_on = True
            intentional_disconnect = False
            set_managed_target(guild.id, target.id, target.name)
            if not await send_voice_state(guild.id, target.id):
                is_muted, is_deafened, is_streaming, is_camera_on = old_state
                set_managed_target()
                update_all_ui()
                return await ctx.edit_v2("Connection Failed", "Discord rejected the voice join request.")

            joined = False
            for _ in range(100):
                current = get_current_voice_channel()
                if current and str(current.id) == str(target.id):
                    joined = True
                    break
                await asyncio.sleep(0.1)

            stream_started = False
            if joined:
                for _ in range(3):
                    if await send_stream_state(guild.id, target.id, True):
                        stream_started = True
                        break
                    await asyncio.sleep(0.25)
            else:
                set_managed_target()

            is_streaming = stream_started
            data = load_data()
            data["settings"].update({
                "muted": True,
                "deafened": True,
                "streaming": stream_started,
                "camera_on": True
            })
            save_data(data)
            await sync_voice_state(update_ui=True)
            if joined:
                await sync_managed_voice_controls(
                    mute=True,
                    deaf=True,
                    stream=stream_started,
                    video=True,
                )

            if not joined:
                await ctx.edit_v2("Connection Failed", f"Discord did not confirm **#{target.name}**.")
            elif stream_started:
                await ctx.edit_v2(
                    "Voice Channel Loaded",
                    f"Joined **#{target.name}** with mute, deafen, stream, and camera enabled."
                )
            else:
                await ctx.edit_v2(
                    "Voice Channel Loaded",
                    f"Joined **#{target.name}** with mute, deafen, and camera. Discord rejected screen share."
                )

        @vc_group.command(name="leave", description="Disconnect the VC Manager voice session")
        async def slash_vc_leave(ctx):
            if not await slash_owner_only(ctx):
                return
            if not is_actually_connected():
                return await ctx.error("VC Manager is not connected.")
            await ctx.send_v2("VC Manager", "Disconnecting the managed voice session...")
            if await disconnect_from_voice(intentional=True):
                await ctx.edit_v2("Disconnected", "VC Manager left the voice channel.")
            else:
                await ctx.error("Could not send the disconnect request.")

        async def slash_voice_set(ctx, setting, enabled, current_enabled):
            if not await slash_owner_only(ctx):
                return
            if not is_actually_connected():
                return await ctx.error("Connect through VC Manager first.")
            label = {
                "mute": "Microphone mute",
                "deaf": "Audio deafen",
                "stream": "Screen share",
                "video": "Camera"
            }[setting]
            if current_enabled == enabled:
                current_state = "enabled" if enabled else "disabled"
                return await ctx.send_v2(
                    f"{label} Unchanged",
                    f"{label} is already **{current_state}**."
                )
            await ctx.send_v2("VC Manager", "Updating the managed voice state...")
            kwargs = {setting: enabled}
            if await update_voice_state(**kwargs):
                await ctx.edit_v2("Voice Control Updated", f"**{label}:** {'Enabled' if enabled else 'Disabled'}")
            else:
                await ctx.error("Discord rejected the voice-state update.")

        @vc_group.command(
            name="mute",
            description="Enable or disable microphone mute for the managed session",
            describe={"state": "Choose whether microphone mute should be enabled or disabled"}
        )
        async def slash_vc_mute(ctx, state: Literal["enable", "disable"]):
            await slash_voice_set(ctx, "mute", state == "enable", is_muted)

        @vc_group.command(
            name="deafen",
            description="Enable or disable audio deafen for the managed session",
            describe={"state": "Choose whether audio deafen should be enabled or disabled"}
        )
        async def slash_vc_deafen(ctx, state: Literal["enable", "disable"]):
            await slash_voice_set(ctx, "deaf", state == "enable", is_deafened)

        @vc_group.command(
            name="stream",
            description="Enable or disable screen share for the managed session",
            describe={"state": "Choose whether screen share should be enabled or disabled"}
        )
        async def slash_vc_stream(ctx, state: Literal["enable", "disable"]):
            await slash_voice_set(ctx, "stream", state == "enable", is_streaming)

        @vc_group.command(
            name="camera",
            description="Enable or disable camera video for the managed session",
            describe={"state": "Choose whether the camera should be enabled or disabled"}
        )
        async def slash_vc_camera(ctx, state: Literal["enable", "disable"]):
            await slash_voice_set(ctx, "video", state == "enable", is_camera_on)

        @vc_group.command(name="list", description="List users in the managed voice channel")
        async def slash_vc_list(ctx):
            if not await slash_owner_only(ctx):
                return
            current = get_current_voice_channel()
            if not current:
                return await ctx.error("Connect through VC Manager first.")
            members = list(getattr(current, "members", []))
            names = "\n".join(f"- {getattr(member, 'display_name', member.name)}" for member in members)
            await ctx.send_v2(
                f"#{current.name}",
                f"**Participants:** {len(members)}\n{names or 'No active users'}"
            )

        @vc_group.command(name="history", description="Show recent VC Manager sessions")
        async def slash_vc_history(ctx):
            if not await slash_owner_only(ctx):
                return
            sessions = get_primary_session_history(include_live=True)[-5:]
            if not sessions:
                return await ctx.send_v2("Session History", "No voice sessions.")
            lines = [
                f"**#{session.get('channel_name', '?')}** in **{session.get('guild_name', '?')}**\n"
                f"Joined: {format_session_time(session.get('start_time'))}\n"
                f"Left: {format_session_time(session.get('end_time') or session.get('timestamp')) if (session.get('end_time') or session.get('timestamp')) else 'Still connected'}\n"
                f"Duration: {format_duration(session.get('duration_seconds', 0))}"
                for session in reversed(sessions)
            ]
            await ctx.send_v2("Recent Voice Sessions", "\n\n".join(lines))

        @vc_group.command(name="status", description="Show the managed voice connection state")
        async def slash_vc_status(ctx):
            if not await slash_owner_only(ctx):
                return
            current = get_current_voice_channel()
            if not current:
                return await ctx.send_v2("VC Manager Status", "**State:** Disconnected")
            session_seconds = (
                (datetime.now() - connection_start_time).total_seconds()
                if connection_start_time else 0
            )
            slash_settings = get_slash_response_settings()
            if not slash_settings["detailed_status"]:
                return await ctx.send_v2(
                    "VC Manager Status",
                    f"**State:** Connected\n**Channel:** #{current.name}"
                )
            await ctx.send_v2(
                "VC Manager Status",
                f"**State:** Connected\n**Server:** {current.guild.name}\n"
                f"**Channel:** #{current.name}\n**Channel ID:** `{current.id}`\n"
                f"**Session:** {format_duration(session_seconds)}"
            )

        @vc_group.command(name="stats", description="Show tracked VC Manager voice statistics")
        async def slash_vc_stats(ctx):
            if not await slash_owner_only(ctx):
                return

            data = load_data()
            sessions = [
                session for session in data.get("sessions", [])
                if isinstance(session, dict)
            ]
            completed_durations = []
            for session in sessions:
                try:
                    duration = max(0.0, float(session.get("duration_seconds", 0) or 0))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(duration):
                    completed_durations.append(duration)

            current = get_current_voice_channel()
            live_seconds = (
                max(0.0, (datetime.now() - connection_start_time).total_seconds())
                if current and connection_start_time else 0.0
            )
            try:
                recorded_total = max(0.0, float(data.get("total_vc_time_seconds", 0) or 0))
            except (TypeError, ValueError):
                recorded_total = sum(completed_durations)

            total_seconds = recorded_total + live_seconds
            longest_seconds = max(completed_durations + ([live_seconds] if live_seconds else []), default=0)
            average_seconds = (
                sum(completed_durations) / len(completed_durations)
                if completed_durations else 0
            )
            server_count = len({
                str(session.get("guild_name") or session.get("guild_id") or "").strip()
                for session in sessions
                if str(session.get("guild_name") or session.get("guild_id") or "").strip()
            })
            connected_accounts = sum(client.is_in_vc for client in token_clients.values())
            current_text = (
                f"#{current.name} in {current.guild.name}"
                if current else "Disconnected"
            )

            await ctx.send_v2(
                "VC Manager Statistics",
                f"**Total voice time:** {format_duration(total_seconds)}\n"
                f"**Current session:** {format_duration(live_seconds)}\n"
                f"**Current channel:** {current_text}\n"
                f"**Completed sessions:** {len(completed_durations)}\n"
                f"**Longest session:** {format_duration(longest_seconds)}\n"
                f"**Average session:** {format_duration(average_seconds)}\n"
                f"**Servers visited:** {server_count}\n"
                f"**Managed accounts connected:** {connected_accounts} / {len(token_clients)}"
            )

        @vc_group.command(
            name="settings",
            description="View or update slash response settings",
            describe={
                "ephemeral": "True makes slash responses visible only to you",
                "theme": "True applies your active Nighty Components V2 theme",
                "detailed": "True includes IDs, server names, and session time in status"
            }
        )
        async def slash_vc_settings(
            ctx,
            ephemeral: bool = None,
            theme: bool = None,
            detailed: bool = None
        ):
            if not await slash_owner_only(ctx):
                return
            settings = get_slash_response_settings()
            changed = []
            if ephemeral is not None:
                settings["ephemeral"] = ephemeral
                changed.append("Ephemeral responses")
            if theme is not None:
                settings["use_theme"] = theme
                changed.append("Nighty V2 theme")
            if detailed is not None:
                settings["detailed_status"] = detailed
                changed.append("Detailed status")
            if changed:
                save_slash_response_settings(settings)
            sync_command_settings_ui(settings)

            def setting_label(value):
                return "Enabled" if value else "Disabled"

            heading = "Saved your settings.\n\n" if changed else ""
            await ctx.send_v2(
                "VC Manager Settings",
                heading
                + f"**Ephemeral responses:** {setting_label(settings['ephemeral'])}\n"
                + f"**Nighty V2 theme:** {setting_label(settings['use_theme'])}\n"
                + f"**Detailed status:** {setting_label(settings['detailed_status'])}\n\n"
                + "Use `/vc settings` options to change these values."
            )

        @vc_group.command(name="help", description="Show all VC Manager slash commands")
        async def slash_vc_help(ctx):
            if not await slash_owner_only(ctx):
                return
            await ctx.send_v2(
                "VC Manager Commands",
                "`/vc join`: Join by channel ID or name\n"
                "`/vc load`: Join with all voice controls enabled\n"
                "`/vc leave`: Disconnect the managed session\n"
                "`/vc mute state:<enable|disable>`: Set microphone mute\n"
                "`/vc deafen state:<enable|disable>`: Set audio deafen\n"
                "`/vc stream state:<enable|disable>`: Set screen share\n"
                "`/vc camera state:<enable|disable>`: Set the camera state\n"
                "`/vc list`: List channel participants\n"
                "`/vc history`: Show recent sessions\n"
                "`/vc status`: Show connection details\n"
                "`/vc stats`: Show tracked voice usage statistics\n"
                "`/vc settings`: View or update response settings"
            )

        slash.autosync()

    # ==================== UI CONSTRUCTION ====================

    tab = Tab(name="VC Manager", icon="users", gap=4)

    # 1. MAIN 2-COLUMN GRID
    main = tab.create_container(type="columns", width="full", height="auto", gap=4)

    # ===== COLUMN 1 (LEFT) =====
    col1 = main.create_container(type="rows", width="auto", height="auto", gap=4)

    # Primary connection card
    connection_card = col1.create_card(type="rows", height="auto", gap=3, disallow_shrink=True)
    connection_card.create_ui_element(
        UI.Text,
        content=f"VC Manager v{SCRIPT_VERSION}",
        size="lg",
        weight="bold",
    )

    initial_guild_items = [
        {
            "id": str(g.id),
            "title": g.name,
            "iconUrl": get_guild_icon_url(g)
        }
        for g in getattr(bot, "guilds", [])
    ]
    initial_servers = [{"id": "none", "title": "Select a server"}] + initial_guild_items
    if not initial_guild_items:
        initial_servers = [{"id": "none", "title": "No servers found"}]
    server_select = connection_card.create_ui_element(
        UI.Select,
        label="Select Server",
        items=initial_servers,
        selected_items=["none"],
        mode="single",
        full_width=True
    )

    channel_select = connection_card.create_ui_element(
        UI.Select,
        label="Select Voice Channel",
        items=[{"id": "none", "title": "Select a server first"}],
        selected_items=["none"],
        mode="single",
        full_width=True
    )

    direct_channel_input = connection_card.create_ui_element(
        UI.Input,
        label="Direct Channel ID (Optional)",
        placeholder="e.g. 123456789012345678",
        full_width=True
    )

    btn_group = connection_card.create_group(type="columns", gap=2)
    join_button = btn_group.create_ui_element(UI.Button, label="Join Voice", variant="solid", color="primary")
    leave_button = btn_group.create_ui_element(UI.Button, label="Disconnect", variant="solid", color="danger", disabled=True)
    refresh_list_button = btn_group.create_ui_element(UI.Button, label="Refresh", variant="ghost", color="default")

    ui_refs["server_select"] = server_select
    ui_refs["channel_select"] = channel_select
    ui_refs["direct_channel_input"] = direct_channel_input
    ui_refs["join_button"] = join_button
    ui_refs["leave_button"] = leave_button

    # Participants Card
    participants_card = col1.create_card(type="rows", height="auto", gap=3, disallow_shrink=True)
    participants_title = participants_card.create_ui_element(UI.Text, content="Channel Participants (0)", size="base", weight="bold")
    ui_refs["participants_title"] = participants_title

    participant_pager_id = f"vc-participant-pager-{id(participants_card)}"
    participants_pager = participants_card.create_ui_element(
        UI.ParticipantPager,
        pager_id=participant_pager_id
    )
    ui_refs["participants_pager"] = participants_pager

    # ===== COLUMN 2 (RIGHT) =====
    col2 = main.create_container(type="rows", width="full", height="auto", gap=4)

    # Place connection and channel status on one row.
    status_row = col2.create_container(type="columns", width="full", height="auto", gap=4)
    # Give Voice Status its content width and Channel Status the remaining space.
    status_card = status_row.create_card(type="rows", width="auto", height="auto", gap=2, disallow_shrink=True)
    status_card.create_ui_element(UI.Text, content="Voice Status", size="base", weight="bold")

    status_text = status_card.create_ui_element(UI.Text, content="🔴  Disconnected", size="base", weight="bold")
    guild_name_text = status_card.create_ui_element(UI.Text, content="Server: -", size="sm")
    channel_name_text = status_card.create_ui_element(UI.Text, content="Channel: -", size="sm")
    channel_id_text = status_card.create_ui_element(UI.Text, content="ID: -", size="sm")
    session_time_text = status_card.create_ui_element(UI.Text, content="Session: 0s", size="sm")
    total_time_text = status_card.create_ui_element(UI.Text, content="Total Time: 0s", size="sm")

    modal_btn_group = status_card.create_group(type="columns", gap=2)
    details_btn = modal_btn_group.create_ui_element(UI.Button, label="Channel Details", variant="bordered", color="default")
    logs_btn = modal_btn_group.create_ui_element(UI.Button, label="Session Logs", variant="bordered", color="default")

    ui_refs["status_text"] = status_text
    ui_refs["guild_name_text"] = guild_name_text
    ui_refs["channel_name_text"] = channel_name_text
    ui_refs["channel_id_text"] = channel_id_text
    ui_refs["session_time_text"] = session_time_text
    ui_refs["total_time_text"] = total_time_text

    channel_status_card = status_row.create_card(type="rows", width="full", height="auto", gap=2)
    channel_status_card.create_ui_element(UI.Text, content="Channel Status", size="base", weight="bold")
    voice_status_input = channel_status_card.create_ui_element(
        UI.Input,
        label="Voice Channel Status",
        placeholder="Enter a status...",
        full_width=True,
        disabled=True
    )
    voice_status_hint = channel_status_card.create_ui_element(
        UI.Text,
        content="Connect through VC Manager to edit the channel status.",
        size="sm"
    )
    channel_status_buttons = channel_status_card.create_group(type="columns", gap=2)
    set_channel_status_btn = channel_status_buttons.create_ui_element(
        UI.Button, label="Set Status", variant="solid", color="primary", disabled=True
    )
    clear_channel_status_btn = channel_status_buttons.create_ui_element(
        UI.Button, label="Clear", variant="bordered", color="default", disabled=True
    )

    ui_refs["voice_status_input"] = voice_status_input
    ui_refs["voice_status_hint"] = voice_status_hint
    ui_refs["set_channel_status_btn"] = set_channel_status_btn
    ui_refs["clear_channel_status_btn"] = clear_channel_status_btn

    # Voice Controls Card
    controls_card = col2.create_card(type="rows", height="auto", gap=3, disallow_shrink=True)
    controls_card.create_ui_element(UI.Text, content="Voice Controls", size="lg", weight="bold")

    mute_toggle = controls_card.create_ui_element(UI.Toggle, label="Microphone Mute", checked=False)
    deafen_toggle = controls_card.create_ui_element(UI.Toggle, label="Audio Deafen", checked=False)
    stream_toggle = controls_card.create_ui_element(UI.Toggle, label="Screen Share (Stream)", checked=False)
    camera_toggle = controls_card.create_ui_element(UI.Toggle, label="Video Camera", checked=False)

    ui_refs["mute_toggle"] = mute_toggle
    ui_refs["deafen_toggle"] = deafen_toggle
    ui_refs["stream_toggle"] = stream_toggle
    ui_refs["camera_toggle"] = camera_toggle

    # Place automation and command settings on one row.
    automation_settings_row = col2.create_container(type="columns", width="full", height="auto", gap=4)

    # Automation & Timers Card
    # Full width gives both cards the same flex basis. CUI can measure a hidden
    # auto-width pager as a few pixels after a browser refresh.
    automation_card = automation_settings_row.create_card(
        type="rows", width="full", height="auto", vertical_align="start", gap=3
    )
    automation_pager = CardPager(automation_card, max_height=300)
    automation_page = automation_pager.create_page()
    automation_page.create_ui_element(UI.Text, content="Automation & Timers", size="lg", weight="bold")

    reconnect_toggle = automation_page.create_ui_element(UI.Toggle, label="Auto-Reconnect on Drop", checked=False)
    leave_alone_toggle = automation_page.create_ui_element(UI.Toggle, label="Leave When Alone in VC", checked=False)
    follow_toggle = automation_page.create_ui_element(UI.Toggle, label="Auto-Follow User", checked=False)
    follow_target_input = automation_page.create_ui_element(
        UI.Input, label="Target User ID", placeholder="Enter Discord User ID...", full_width=True
    )

    automation_page.create_ui_element(UI.Space, height="20px")
    pending_timer_minutes = timer_minutes(load_data().get("settings", {}).get("auto_disconnect_minutes"))

    def handle_timer_slider(value):
        nonlocal pending_timer_minutes
        try:
            pending_timer_minutes = max(0, min(120, int(float(value))))
        except (TypeError, ValueError):
            pending_timer_minutes = 0

    timer_group = automation_page.create_group(type="columns", gap=2)
    timer_slider = timer_group.create_ui_element(
        UI.Slider,
        label="Disconnect timer",
        min_val=0,
        max_val=120,
        value=pending_timer_minutes,
        on_change=handle_timer_slider
    )
    apply_timer_btn = timer_group.create_ui_element(UI.Button, label="Save timer", variant="solid", color="primary")
    automation_pager.build()

    ui_refs["reconnect_toggle"] = reconnect_toggle
    ui_refs["leave_alone_toggle"] = leave_alone_toggle
    ui_refs["follow_toggle"] = follow_toggle
    ui_refs["follow_target_input"] = follow_target_input
    ui_refs["timer_slider"] = timer_slider

    # Command Settings Card
    command_settings = get_slash_response_settings()
    command_settings_card = automation_settings_row.create_card(
        type="rows", width="full", height="auto", vertical_align="start", gap=3
    )
    command_settings_card.create_ui_element(
        UI.Text, content="Command Settings", size="lg", weight="bold"
    )
    command_ephemeral_toggle = command_settings_card.create_ui_element(
        UI.Toggle,
        label="Ephemeral Responses",
        checked=command_settings["ephemeral"],
    )
    command_theme_toggle = command_settings_card.create_ui_element(
        UI.Toggle,
        label="Nighty V2 Theme",
        checked=command_settings["use_theme"],
    )
    command_detailed_toggle = command_settings_card.create_ui_element(
        UI.Toggle,
        label="Detailed Status",
        checked=command_settings["detailed_status"],
    )
    command_settings_card.create_ui_element(
        UI.Text,
        content="Use these toggles for /vc settings.",
        size="sm",
    )

    ui_refs["command_ephemeral_toggle"] = command_ephemeral_toggle
    ui_refs["command_theme_toggle"] = command_theme_toggle
    ui_refs["command_detailed_toggle"] = command_detailed_toggle

    # 2. MULTI-TOKEN CONTAINER
    multi_token_container = tab.create_container(type="rows", width="full", height="auto", gap=4)
    multi_token_setup_row = multi_token_container.create_container(
        type="columns", width="full", height="auto", gap=4
    )

    token_card = multi_token_setup_row.create_card(type="rows", width="full", height="auto", gap=3)
    token_pager = CardPager(token_card, max_height=320)
    token_pager_page = token_pager.create_page()
    token_add_page = token_pager_page.create_group(type="rows", gap=3, full_width=True)
    token_add_page.create_ui_element(UI.Text, content="Multi-Token Accounts", size="lg", weight="bold")
    token_add_page.create_ui_element(UI.Text, content="Connect secondary user accounts", size="sm")

    token_input = token_add_page.create_ui_element(UI.Input, label="User Token", placeholder="Paste Discord token...", full_width=True)
    token_name_input = token_add_page.create_ui_element(UI.Input, label="Account Label", placeholder="e.g. Alt #1", full_width=True)

    token_btn_group = token_add_page.create_group(type="columns", gap=2)
    add_token_btn = token_btn_group.create_ui_element(UI.Button, label="Add Account", variant="solid", color="primary")

    token_bulk_page = token_pager_page.create_group(type="rows", gap=3, full_width=True)
    token_bulk_page.create_ui_element(UI.Text, content="Bulk Account Controls", size="lg", weight="bold")
    token_bulk_page.create_ui_element(
        UI.Text,
        content="Join saved destinations and apply one voice state to every managed account.",
        size="sm",
    )
    bulk_voice_state = {
        "mute": bool(is_muted),
        "deaf": bool(is_deafened),
        "stream": bool(is_streaming),
        "video": bool(is_camera_on),
    }
    bulk_mute_toggle = token_bulk_page.create_ui_element(UI.Toggle, label="Microphone Mute", checked=is_muted)
    bulk_deafen_toggle = token_bulk_page.create_ui_element(UI.Toggle, label="Audio Deafen", checked=is_deafened)
    bulk_stream_toggle = token_bulk_page.create_ui_element(UI.Toggle, label="Screen Share (Stream)", checked=is_streaming)
    bulk_camera_toggle = token_bulk_page.create_ui_element(UI.Toggle, label="Video Camera", checked=is_camera_on)
    token_bulk_actions = token_bulk_page.create_group(type="columns", gap=2)
    conn_all_btn = token_bulk_actions.create_ui_element(UI.Button, label="Join All", variant="solid", color="success")
    apply_all_controls_btn = token_bulk_actions.create_ui_element(
        UI.Button, label="Apply Controls", variant="solid", color="primary"
    )
    disc_all_btn = token_bulk_actions.create_ui_element(UI.Button, label="Leave All", variant="solid", color="danger")
    token_pager.build()

    ui_refs["token_input"] = token_input
    ui_refs["token_name_input"] = token_name_input

    destination_card = multi_token_setup_row.create_card(
        type="rows", width="full", height="auto", gap=3
    )
    destination_card.create_ui_element(
        UI.Text, content="Account Destination", size="lg", weight="bold"
    )
    destination_card.create_ui_element(
        UI.Text,
        content="Choose a server and voice channel for one managed account.",
        size="sm",
    )
    saved_account_items = []
    for saved_item in load_tokens().get("tokens", []):
        if isinstance(saved_item, dict) and saved_item.get("id"):
            saved_account_items.append({
                "id": str(saved_item["id"]),
                "title": str(saved_item.get("label") or saved_item.get("name") or "Managed account"),
            })
    if not saved_account_items:
        saved_account_items = [{"id": "none", "title": "Add an account first"}]

    destination_account_select = destination_card.create_ui_element(
        UI.Select,
        label="Managed Account",
        items=saved_account_items,
        selected_items=["none"] if saved_account_items[0]["id"] == "none" else [],
        mode="single",
        full_width=True,
    )
    destination_server_select = destination_card.create_ui_element(
        UI.Select,
        label="Server",
        items=[{"id": "none", "title": "Select an account first"}],
        selected_items=["none"],
        mode="single",
        full_width=True,
        disabled=True,
    )
    destination_channel_select = destination_card.create_ui_element(
        UI.Select,
        label="Voice Channel",
        items=[{"id": "none", "title": "Select a server first"}],
        selected_items=["none"],
        mode="single",
        full_width=True,
        disabled=True,
    )
    destination_actions = destination_card.create_group(type="columns", gap=2)
    destination_join_button = destination_actions.create_ui_element(
        UI.Button, label="Join Channel", variant="solid", color="primary", disabled=True
    )
    destination_refresh_button = destination_actions.create_ui_element(
        UI.Button, label="Refresh", variant="bordered", color="default", disabled=True
    )
    destination_hint = destination_card.create_ui_element(
        UI.Text,
        content="Each account keeps its own destination and reconnects there after recoverable network drops.",
        size="sm",
    )

    ui_refs["destination_account_select"] = destination_account_select
    ui_refs["destination_server_select"] = destination_server_select
    ui_refs["destination_channel_select"] = destination_channel_select
    ui_refs["destination_join_button"] = destination_join_button
    ui_refs["destination_refresh_button"] = destination_refresh_button
    ui_refs["destination_hint"] = destination_hint

    tokens_table_card = multi_token_container.create_card(type="rows", width="full", height="auto", gap=3)
    tokens_table_card.create_ui_element(UI.Text, content="Managed Accounts", size="lg", weight="bold")

    managed_accounts_pager_id = f"vc-managed-accounts-pager-{id(tokens_table_card)}"
    managed_accounts_pager = tokens_table_card.create_ui_element(
        UI.ManagedAccountsPager,
        pager_id=managed_accounts_pager_id,
        on_action=lambda payload: handle_token_action(payload)
    )
    ui_refs["managed_accounts_pager"] = managed_accounts_pager

    # ==================== UI UPDATE & LOGIC HANDLERS ====================

    def update_participants_table():
        cur = get_current_voice_channel()
        if not cur:
            ui_refs["participants_title"].content = "Channel Participants (0)"
            evaluate_js(
                f"if (window.VCParticipantPager) "
                f"window.VCParticipantPager.update({json.dumps(participant_pager_id)}, []);"
            )
            return

        members = list(getattr(cur, "members", []) or [])
        if not members:
        # Nighty may populate channel.members after rendering the tab. Read the
        # guild voice-state cache so the first paint includes channel members.
            guild = getattr(cur, "guild", None)
            voice_states = getattr(guild, "_voice_states", {}) if guild else {}
            cached_members = []
            for user_id, voice_state in voice_states.items():
                state_channel = getattr(voice_state, "channel", None)
                state_channel_id = getattr(state_channel, "id", None)
                if state_channel_id is None:
                    state_channel_id = getattr(voice_state, "channel_id", None)
                if str(state_channel_id or "") != str(cur.id):
                    continue
                try:
                    member = guild.get_member(int(user_id))
                except (TypeError, ValueError):
                    member = None
                if member:
                    cached_members.append(member)
            if cached_members:
                members = cached_members
        ui_refs["participants_title"].content = f"Channel Participants ({len(members)})"

        rows = []
        for m in members:
            vs = getattr(m, "voice", None)
            active_states = []

            if vs:
                if getattr(vs, "self_mute", False) or getattr(vs, "mute", False):
                    active_states.append("Mute")
                if getattr(vs, "self_deaf", False) or getattr(vs, "deaf", False):
                    active_states.append("Deaf")
                if getattr(vs, "self_stream", False) or getattr(vs, "streaming", False):
                    active_states.append("Stream")
                if getattr(vs, "self_video", False):
                    active_states.append("Camera")

            voice_state_text = " | ".join(active_states) if active_states else "Active"
            avatar_url = ""
            if hasattr(m, "display_avatar") and m.display_avatar:
                avatar_url = str(m.display_avatar.url)
            elif hasattr(m, "avatar") and m.avatar:
                avatar_url = str(m.avatar.url)

            rows.append({
                "id": str(m.id),
                "name": getattr(m, "display_name", m.name),
                "avatar_url": avatar_url,
                "voice_state": voice_state_text
            })

        evaluate_js(
            f"if (window.VCParticipantPager) "
            f"window.VCParticipantPager.update({json.dumps(participant_pager_id)}, {json.dumps(rows)});"
        )

    def update_all_ui():
        nonlocal status_editor_channel_id
        try:
            cur = get_current_voice_channel()
            connected = cur is not None

            for key, checked in (
                ("mute_toggle", is_muted),
                ("deafen_toggle", is_deafened),
                ("stream_toggle", is_streaming),
                ("camera_toggle", is_camera_on),
            ):
                if key in ui_refs:
                    ui_refs[key].checked = checked if connected else False
                    ui_refs[key].disabled = not connected

            if connected:
                g_name = cur.guild.name
                c_name = cur.name
                c_id = str(cur.id)
                ui_refs["status_text"].content = "🟢  Connected"
                ui_refs["guild_name_text"].content = f"Server: {g_name}"
                ui_refs["channel_name_text"].content = f"Channel: #{c_name}"
                ui_refs["channel_id_text"].content = f"ID: {c_id}"

                ui_refs["join_button"].disabled = True
                ui_refs["leave_button"].disabled = False

                can_edit_status = can_set_voice_channel_status(cur)
                ui_refs["voice_status_input"].disabled = not can_edit_status
                ui_refs["set_channel_status_btn"].disabled = not can_edit_status
                ui_refs["clear_channel_status_btn"].disabled = not can_edit_status
                if status_editor_channel_id != c_id:
                    current_status = (
                        getattr(cur, "voice_status", None)
                        or getattr(cur, "status", None)
                        or ""
                    )
                    ui_refs["voice_status_input"].value = str(current_status)
                    status_editor_channel_id = c_id
                ui_refs["voice_status_hint"].content = (
                    "Set or clear the status shown beneath this voice channel."
                    if can_edit_status
                    else "You do not have the Voice Channel Status permission."
                )

            else:
                ui_refs["status_text"].content = "🔴  Disconnected"
                ui_refs["guild_name_text"].content = "Server: -"
                ui_refs["channel_name_text"].content = "Channel: -"
                ui_refs["channel_id_text"].content = "ID: -"

                ui_refs["join_button"].disabled = False
                ui_refs["leave_button"].disabled = True
                ui_refs["voice_status_input"].value = ""
                ui_refs["voice_status_input"].disabled = True
                ui_refs["set_channel_status_btn"].disabled = True
                ui_refs["clear_channel_status_btn"].disabled = True
                ui_refs["voice_status_hint"].content = "Connect through VC Manager to edit the channel status."
                status_editor_channel_id = None

            # Update session time
            session_sec = (datetime.now() - connection_start_time).total_seconds() if (connected and connection_start_time) else 0
            data = load_data()
            total_sec = data.get("total_vc_time_seconds", 0) + session_sec

            ui_refs["session_time_text"].content = f"Session: {format_duration(session_sec)}"
            ui_refs["total_time_text"].content = f"Total Time: {format_duration(total_sec)}"

            update_participants_table()

        except Exception as e:
            print(f"[VC Manager] Error updating UI: {e}", type_="ERROR")

    def update_token_table_rows():
        data = load_tokens()
        rows = []
        if data.get("_load_error"):
            rows.append({
                "id": "storage-error",
                "account_id": "-",
                "name": "Token file error",
                "avatar_url": "",
                "server": "-",
                "channel": "-",
                "status": "Not loaded",
                "error": data["_load_error"],
                "connected": False,
                "muted": False,
                "deafened": False,
                "streaming": False,
                "camera_on": False,
                "removable": False
            })
        for item in data.get("tokens", []):
            if not isinstance(item, dict):
                continue
            tid = str(item.get("id") or "")
            name = str(item.get("label") or item.get("name") or "Account")
            client = token_clients.get(tid)

            in_vc = client.is_in_vc if client else False
            status_text = "Connected" if in_vc else (getattr(client, "status", "Idle") if client else "Not loaded")
            error_text = getattr(client, "last_error", "") if client else "Nighty did not create a gateway client for this saved account. Remove it and add it again."
            server_info = "-"
            ch_info = "-"
            if client and client.current_guild_id:
                current_guild_id = str(client.current_guild_id)
                cached_guild_name = next(
                    (
                        guild_item.get("name")
                        for guild_item in client.guild_cache
                        if str(guild_item.get("id")) == current_guild_id
                    ),
                    None,
                )
                if not cached_guild_name:
                    try:
                        primary_guild = bot.get_guild(int(current_guild_id))
                        cached_guild_name = getattr(primary_guild, "name", None)
                    except (TypeError, ValueError):
                        cached_guild_name = None
                server_info = str(cached_guild_name or f"ID: {current_guild_id}")
            if client and client.current_channel_id:
                channel = bot.get_channel(int(client.current_channel_id))
                if channel:
                    ch_info = f"#{channel.name}"
                else:
                    cached_name = next(
                        (
                            channel_item.get("name")
                            for channel_items in client.channel_cache.values()
                            for channel_item in channel_items
                            if str(channel_item.get("id")) == str(client.current_channel_id)
                        ),
                        None,
                    )
                    ch_info = f"#{cached_name}" if cached_name else f"ID: {client.current_channel_id}"
            elif item.get("preferred_channel_id"):
                ch_info = "Saved destination"

            rows.append({
                "id": str(tid),
                "account_id": str(client.user_id) if (client and client.user_id) else f"{tid[:8]}...",
                "name": name,
                "avatar_url": getattr(client, "avatar_url", "") if client else "",
                "server": server_info,
                "channel": ch_info,
                "status": status_text,
                "error": error_text,
                "connected": in_vc,
                "muted": bool(client.is_muted) if client else False,
                "deafened": bool(client.is_deafened) if client else False,
                "streaming": bool(client.is_streaming) if client else False,
                "camera_on": bool(client.is_camera_on) if client else False,
                "removable": True
            })
        evaluate_js(
            f"if (window.VCManagedAccountsPager) "
            f"window.VCManagedAccountsPager.update({json.dumps(managed_accounts_pager_id)}, {json.dumps(rows)});"
        )

    async def refresh_channel_list(guild_id, preserve_selection=True):
        nonlocal _updating_dropdowns, selected_channel_id
        try:
            guild = bot.get_guild(int(guild_id))
            if not guild:
                ui_refs["channel_select"].items = [{"id": "none", "title": "Server not accessible"}]
                ui_refs["channel_select"].selected_items = ["none"]
                return

            vcs = list(getattr(guild, "voice_channels", []))
            known_ids = {getattr(channel, "id", None) for channel in vcs}
            for channel in getattr(guild, "stage_channels", []):
                if getattr(channel, "id", None) not in known_ids:
                    vcs.append(channel)
                    known_ids.add(getattr(channel, "id", None))
            if not vcs:
                for channel in getattr(guild, "channels", []):
                    channel_type = str(getattr(channel, "type", "")).lower()
                    if "voice" in channel_type or "stage" in channel_type or hasattr(channel, "bitrate"):
                        vcs.append(channel)

            items = []
            for voice_channel in vcs:
                member_count = len(getattr(voice_channel, "members", []))
                user_limit = int(getattr(voice_channel, "user_limit", 0) or 0)
                limit_text = str(user_limit) if user_limit > 0 else "∞"
                items.append({
                    "id": str(voice_channel.id),
                    "title": f"{voice_channel.name} ({member_count}/{limit_text})"
                })
            if not items:
                items = [{"id": "none", "title": "No voice channels found"}]

            _updating_dropdowns = True
            ui_refs["channel_select"].items = items

            previous = selected_item_id(ui_refs["channel_select"].selected_items)
            valid_ids = {i["id"] for i in items}

            if preserve_selection and previous in valid_ids:
                ui_refs["channel_select"].selected_items = [previous]
                selected_channel_id = previous
            elif items and items[0]["id"] != "none":
                ui_refs["channel_select"].selected_items = [items[0]["id"]]
                selected_channel_id = items[0]["id"]
            else:
                ui_refs["channel_select"].selected_items = ["none"]
                selected_channel_id = None

        except Exception as e:
            print(f"[VC Manager] Error refreshing channels: {e}", type_="ERROR")
        finally:
            _updating_dropdowns = False

    async def refresh_server_list(preserve_selection=True):
        nonlocal _updating_dropdowns, selected_server_id, selected_channel_id
        try:
            guilds = getattr(bot, "guilds", [])
            guild_items = [
                {
                    "id": str(g.id),
                    "title": g.name,
                    "iconUrl": get_guild_icon_url(g)
                }
                for g in guilds
            ]
            items = [{"id": "none", "title": "Select a server"}] + guild_items
            if not guild_items:
                items = [{"id": "none", "title": "No servers found"}]

            _updating_dropdowns = True
            ui_refs["server_select"].items = items

            previous = selected_item_id(ui_refs["server_select"].selected_items)
            valid_ids = {i["id"] for i in items}

            if preserve_selection and previous in valid_ids:
                ui_refs["server_select"].selected_items = [previous]
                selected_server_id = previous if previous != "none" else None
            else:
                ui_refs["server_select"].selected_items = ["none"]
                selected_server_id = None

            if selected_server_id and selected_server_id != "none":
                await refresh_channel_list(selected_server_id, preserve_selection=preserve_selection)
            else:
                selected_channel_id = None
                ui_refs["channel_select"].items = [{"id": "none", "title": "Select a server first"}]
                ui_refs["channel_select"].selected_items = ["none"]

        except Exception as e:
            print(f"[VC Manager] Error refreshing servers: {e}", type_="ERROR")
        finally:
            _updating_dropdowns = False

    def refresh_destination_account_items(preserve_selection=True):
        nonlocal selected_token_id
        items = []
        for saved_item in load_tokens().get("tokens", []):
            if isinstance(saved_item, dict) and saved_item.get("id"):
                item_id = str(saved_item["id"])
                select_item = {
                    "id": item_id,
                    "title": str(saved_item.get("label") or saved_item.get("name") or "Managed account"),
                }
                client = token_clients.get(item_id)
                if client and client.avatar_url:
                    select_item["iconUrl"] = client.avatar_url
                items.append(select_item)
        ui_refs["destination_account_select"].items = items or [
            {"id": "none", "title": "Add an account first"}
        ]
        valid_ids = {item["id"] for item in items}
        if preserve_selection and selected_token_id in valid_ids:
            ui_refs["destination_account_select"].selected_items = [selected_token_id]
        else:
            selected_token_id = None
            ui_refs["destination_account_select"].selected_items = ["none"] if not items else []
            ui_refs["destination_server_select"].items = [
                {"id": "none", "title": "Select an account first"}
            ]
            ui_refs["destination_server_select"].selected_items = ["none"]
            ui_refs["destination_server_select"].disabled = True
            ui_refs["destination_channel_select"].items = [
                {"id": "none", "title": "Select a server first"}
            ]
            ui_refs["destination_channel_select"].selected_items = ["none"]
            ui_refs["destination_channel_select"].disabled = True
            ui_refs["destination_join_button"].disabled = True
            ui_refs["destination_refresh_button"].disabled = not bool(items)

    def get_saved_token_item(token_id):
        for saved_item in load_tokens().get("tokens", []):
            if isinstance(saved_item, dict) and str(saved_item.get("id") or "") == str(token_id or ""):
                return saved_item
        return None

    def resolve_token_join_destination(token_id):
        """Choose a destination for one managed account without sharing credentials."""
        token_id = str(token_id or "")
        token_data = load_tokens().get("tokens", [])
        saved_item = next(
            (
                item for item in token_data
                if isinstance(item, dict) and str(item.get("id") or "") == token_id
            ),
            {},
        )
        guild_id = str(saved_item.get("preferred_guild_id") or "")
        channel_id = str(saved_item.get("preferred_channel_id") or "")
        if guild_id and channel_id:
            return guild_id, channel_id, "this account's saved destination"

        current = get_current_voice_channel()
        if current and getattr(current, "guild", None):
            return str(current.guild.id), str(current.id), "the primary VC Manager session"

        for other_id, other_client in token_clients.items():
            if (
                str(other_id) != token_id
                and other_client.is_in_vc
                and other_client.current_guild_id
                and other_client.current_channel_id
            ):
                return (
                    str(other_client.current_guild_id),
                    str(other_client.current_channel_id),
                    f"{other_client.name}'s active channel",
                )

        for item in token_data:
            if not isinstance(item, dict) or str(item.get("id") or "") == token_id:
                continue
            shared_guild_id = str(item.get("preferred_guild_id") or "")
            shared_channel_id = str(item.get("preferred_channel_id") or "")
            if shared_guild_id and shared_channel_id:
                shared_name = str(item.get("label") or item.get("name") or "another account")
                return shared_guild_id, shared_channel_id, f"{shared_name}'s saved destination"

        return "", "", ""

    async def refresh_destination_servers(force=False):
        nonlocal selected_token_guild_id, selected_token_channel_id
        client = token_clients.get(str(selected_token_id or ""))
        if not client:
            ui_refs["destination_hint"].content = "This account is not loaded. Remove it and add a fresh token."
            return False
        ui_refs["destination_server_select"].loading = True
        ui_refs["destination_refresh_button"].loading = True
        try:
            if not await client.start_gateway():
                ui_refs["destination_hint"].content = client.last_error or "Discord did not open this account session."
                return False
            guilds = await client.fetch_guilds(force=force)
            if not guilds:
                ui_refs["destination_server_select"].items = [
                    {"id": "none", "title": "No accessible servers"}
                ]
                ui_refs["destination_server_select"].selected_items = ["none"]
                ui_refs["destination_server_select"].disabled = True
                ui_refs["destination_hint"].content = client.last_error or "This account has no accessible servers."
                return False
            server_items = []
            for guild in guilds:
                guild_id = str(guild["id"])
                item = {
                    "id": guild_id,
                    "title": guild["name"],
                    "iconUrl": get_gateway_guild_icon_url(guild),
                }
                server_items.append(item)
            ui_refs["destination_server_select"].items = server_items
            ui_refs["destination_server_select"].disabled = False
            saved_item = get_saved_token_item(selected_token_id) or {}
            preferred = str(saved_item.get("preferred_guild_id") or "")
            valid_ids = {item["id"] for item in server_items}
            selected_token_guild_id = preferred if preferred in valid_ids else None
            ui_refs["destination_server_select"].selected_items = (
                [selected_token_guild_id] if selected_token_guild_id else []
            )
            ui_refs["destination_refresh_button"].disabled = False
            ui_refs["destination_hint"].content = "Select a server, then choose the voice channel for this account."
            if selected_token_guild_id:
                await refresh_destination_channels(selected_token_guild_id, force=force)
            else:
                selected_token_channel_id = None
                ui_refs["destination_channel_select"].items = [
                    {"id": "none", "title": "Select a server first"}
                ]
                ui_refs["destination_channel_select"].selected_items = ["none"]
                ui_refs["destination_channel_select"].disabled = True
                ui_refs["destination_join_button"].disabled = True
            return True
        finally:
            ui_refs["destination_server_select"].loading = False
            ui_refs["destination_refresh_button"].loading = False

    async def refresh_destination_channels(guild_id, force=False):
        nonlocal selected_token_guild_id, selected_token_channel_id
        client = token_clients.get(str(selected_token_id or ""))
        if not client:
            return False
        selected_token_guild_id = str(guild_id or "")
        ui_refs["destination_channel_select"].loading = True
        try:
            channels = await client.fetch_voice_channels(selected_token_guild_id, force=force)
            if not channels:
                ui_refs["destination_channel_select"].items = [
                    {"id": "none", "title": "No accessible voice channels"}
                ]
                ui_refs["destination_channel_select"].selected_items = ["none"]
                ui_refs["destination_channel_select"].disabled = True
                ui_refs["destination_join_button"].disabled = True
                ui_refs["destination_hint"].content = client.last_error or "This account cannot access a voice channel in that server."
                return False
            channel_items = [{"id": item["id"], "title": item["name"]} for item in channels]
            ui_refs["destination_channel_select"].items = channel_items
            ui_refs["destination_channel_select"].disabled = False
            saved_item = get_saved_token_item(selected_token_id) or {}
            preferred = str(saved_item.get("preferred_channel_id") or "")
            valid_ids = {item["id"] for item in channel_items}
            selected_token_channel_id = preferred if preferred in valid_ids else None
            ui_refs["destination_channel_select"].selected_items = (
                [selected_token_channel_id] if selected_token_channel_id else []
            )
            ui_refs["destination_join_button"].disabled = not bool(selected_token_channel_id)
            return True
        finally:
            ui_refs["destination_channel_select"].loading = False

    # Event handlers for UI buttons and toggles
    def handle_bulk_mute_toggle(checked):
        bulk_voice_state["mute"] = as_bool(checked)

    def handle_bulk_deafen_toggle(checked):
        bulk_voice_state["deaf"] = as_bool(checked)

    def handle_bulk_stream_toggle(checked):
        bulk_voice_state["stream"] = as_bool(checked)

    def handle_bulk_camera_toggle(checked):
        bulk_voice_state["video"] = as_bool(checked)

    async def handle_server_select(selected_items):
        nonlocal selected_server_id, selected_channel_id
        value = selected_item_id(selected_items)
        if _updating_dropdowns:
            return
        if value in ("", "none", "loading", "error"):
            selected_server_id = None
            selected_channel_id = None
            ui_refs["channel_select"].items = [{"id": "none", "title": "Select a server first"}]
            ui_refs["channel_select"].selected_items = ["none"]
            return
        selected_server_id = value
        await refresh_channel_list(selected_server_id, preserve_selection=False)

    async def handle_channel_select(selected_items):
        nonlocal selected_channel_id
        value = selected_item_id(selected_items)
        if _updating_dropdowns or value in ("", "none", "loading", "error"):
            return
        selected_channel_id = value

    async def handle_destination_account_select(selected_items):
        nonlocal selected_token_id, selected_token_guild_id, selected_token_channel_id
        token_id = selected_item_id(selected_items)
        selected_token_id = token_id if token_id and token_id != "none" else None
        selected_token_guild_id = None
        selected_token_channel_id = None
        ui_refs["destination_join_button"].disabled = True
        ui_refs["destination_refresh_button"].disabled = not bool(selected_token_id)
        if not selected_token_id:
            refresh_destination_account_items(preserve_selection=False)
            return
        ui_refs["destination_hint"].content = "Loading servers for this account..."
        await refresh_destination_servers(force=False)

    async def handle_destination_server_select(selected_items):
        nonlocal selected_token_guild_id, selected_token_channel_id
        guild_id = selected_item_id(selected_items)
        selected_token_guild_id = guild_id if guild_id and guild_id != "none" else None
        selected_token_channel_id = None
        ui_refs["destination_join_button"].disabled = True
        if not selected_token_guild_id:
            ui_refs["destination_channel_select"].items = [
                {"id": "none", "title": "Select a server first"}
            ]
            ui_refs["destination_channel_select"].selected_items = ["none"]
            ui_refs["destination_channel_select"].disabled = True
            return
        update_saved_token(selected_token_id, preferred_guild_id=selected_token_guild_id)
        await refresh_destination_channels(selected_token_guild_id, force=False)

    async def handle_destination_channel_select(selected_items):
        nonlocal selected_token_channel_id
        channel_id = selected_item_id(selected_items)
        selected_token_channel_id = channel_id if channel_id and channel_id != "none" else None
        ui_refs["destination_join_button"].disabled = not bool(selected_token_channel_id)
        if selected_token_channel_id:
            destination_saved = update_saved_token(
                selected_token_id,
                preferred_guild_id=selected_token_guild_id,
                preferred_channel_id=selected_token_channel_id,
            )
            ui_refs["destination_hint"].content = (
                "Destination saved. Join Channel connects only this managed account."
                if destination_saved
                else "Nighty could not save this destination. Check the scripts JSON folder permissions."
            )

    async def handle_destination_refresh():
        if not selected_token_id:
            tab.toast("Account required", "Select a managed account first.", "ERROR")
            return
        await refresh_destination_servers(force=True)

    async def handle_destination_join():
        client = token_clients.get(str(selected_token_id or ""))
        if not client:
            tab.toast("Account unavailable", "Select a loaded managed account.", "ERROR")
            return
        if not selected_token_guild_id or not selected_token_channel_id:
            tab.toast("Destination required", "Select a server and voice channel for this account.", "ERROR")
            return
        ui_refs["destination_join_button"].loading = True
        try:
            joined = await client.join_vc(
                selected_token_guild_id,
                selected_token_channel_id,
                mute=is_muted,
                deaf=is_deafened,
                stream=is_streaming,
                video=is_camera_on,
            )
            update_token_table_rows()
            if joined:
                channel_name = next(
                    (
                        item.get("name")
                        for item in client.channel_cache.get(str(selected_token_guild_id), [])
                        if str(item.get("id")) == str(selected_token_channel_id)
                    ),
                    selected_token_channel_id,
                )
                if client.last_error:
                    tab.toast("Account connected, destination not saved", client.last_error, "ERROR")
                else:
                    tab.toast("Account connected", f"{client.name} joined #{channel_name}.", "SUCCESS")
            else:
                tab.toast("Account could not join", client.last_error or "Discord did not confirm the voice join.", "ERROR")
        finally:
            ui_refs["destination_join_button"].loading = False

    async def handle_join():
        join_button.loading = True
        try:
            direct_id = (ui_refs["direct_channel_input"].value or "").strip()
            ch_id = direct_id or selected_item_id(ui_refs["channel_select"].selected_items)

            if not ch_id or ch_id in ("none", "loading", "error") or not str(ch_id).strip().isdigit():
                tab.toast("Invalid channel", "Select a voice channel or enter its numeric ID.", "ERROR")
                return

            ch_int = int(str(ch_id).strip())
            guild_id = None
            ch_name = str(ch_id)

            # 1. Check cached channel
            channel = bot.get_channel(ch_int)
            if is_voice_channel(channel):
                guild_id = str(channel.guild.id)
                ch_name = channel.name

        # Search cached guilds before using the selected server.
            if not guild_id:
                for g in getattr(bot, "guilds", []):
                    c = g.get_channel(ch_int)
                    if is_voice_channel(c):
                        guild_id = str(g.id)
                        ch_name = c.name
                        break

        # Fetch a direct ID when the cache misses it.
            if not guild_id:
                try:
                    c = await bot.fetch_channel(ch_int)
                    if is_voice_channel(c):
                        guild_id = str(c.guild.id)
                        ch_name = c.name
                except Exception:
                    pass

        # Use the selected guild for uncached channels.
            if not guild_id and not direct_id:
                selected_guild = selected_item_id(ui_refs["server_select"].selected_items)
                if selected_guild not in ("", "none", "loading", "error"):
                    guild_id = selected_guild

            if not guild_id:
                tab.toast("Server required", "Select the server that contains this voice channel.", "ERROR")
                return

            ok = await connect(guild_id, ch_id)
            if not ok:
                tab.toast("Join failed", "Nighty could not send the voice join request.", "ERROR")
                return

            tab.toast("Connecting", f"Joining #{ch_name}...", "INFO")
            for _ in range(8):
                await asyncio.sleep(0.5)
                if await sync_voice_state(update_ui=True):
                    tab.toast("Connected", f"Joined #{ch_name}", "SUCCESS")
                    break
            else:
                set_managed_target()
                update_all_ui()
                tab.toast("Not connected", f"Discord did not confirm the connection to #{ch_name}", "ERROR")
        except Exception as e:
            tab.toast("Join error", str(e), "ERROR")
        finally:
            join_button.loading = False

    async def handle_leave():
        leave_button.loading = True
        try:
            if await disconnect_from_voice(intentional=True):
                tab.toast("Disconnected", "Left voice channel", "SUCCESS")
            else:
                tab.toast("Not connected", "VC Manager has no active voice connection.", "ERROR")
        except Exception as e:
            tab.toast("Disconnect error", str(e), "ERROR")
        finally:
            leave_button.loading = False

    async def handle_refresh_list():
        refresh_list_button.loading = True
        try:
            await refresh_server_list(preserve_selection=True)
            tab.toast("Channels refreshed", "VC Manager loaded the current channel list.", "SUCCESS")
        except Exception as e:
            tab.toast("Refresh error", str(e), "ERROR")
        finally:
            refresh_list_button.loading = False

    async def handle_mute_toggle(checked):
        if await update_voice_state(mute=checked):
            tab.toast("Microphone", "Muted" if is_muted else "Unmuted", "SUCCESS")
        else:
            tab.toast("Error", "Connect to a voice channel first", "ERROR")

    async def handle_deafen_toggle(checked):
        if await update_voice_state(deaf=checked):
            tab.toast("Audio", "Deafened" if is_deafened else "Undeafened", "SUCCESS")
        else:
            tab.toast("Error", "Connect to a voice channel first", "ERROR")

    async def handle_stream_toggle(checked):
        if await update_voice_state(stream=checked):
            tab.toast("Screen share", "Enabled" if is_streaming else "Disabled", "SUCCESS")
        else:
            tab.toast("Error", "Connect to a voice channel first", "ERROR")

    async def handle_camera_toggle(checked):
        if await update_voice_state(video=checked):
            tab.toast("Camera", "Enabled" if is_camera_on else "Disabled", "SUCCESS")
        else:
            tab.toast("Error", "Connect to a voice channel first", "ERROR")

    async def handle_reconnect_toggle(checked):
        nonlocal is_auto_reconnect
        is_auto_reconnect = as_bool(checked)
        data = load_data()
        data["settings"]["auto_reconnect"] = is_auto_reconnect
        save_data(data)
        tab.toast("Auto-reconnect", "Enabled" if is_auto_reconnect else "Disabled", "SUCCESS")

    async def handle_leave_alone_toggle(checked):
        nonlocal is_leave_when_alone
        is_leave_when_alone = as_bool(checked)
        data = load_data()
        data["settings"]["leave_when_alone"] = is_leave_when_alone
        save_data(data)
        tab.toast("Leave when alone", "Enabled" if is_leave_when_alone else "Disabled", "SUCCESS")

    async def handle_follow_toggle(checked):
        nonlocal is_auto_follow, follow_target_id
        target = (ui_refs["follow_target_input"].value or "").strip()
        enabled = as_bool(checked)
        if enabled and (not target or not target.isdigit()):
            ui_refs["follow_toggle"].checked = False
            tab.toast("Error", "Enter a valid numeric Discord User ID to follow", "ERROR")
            return

        is_auto_follow = enabled
        follow_target_id = target
        data = load_data()
        data["settings"]["auto_follow"] = is_auto_follow
        data["settings"]["follow_target_id"] = follow_target_id
        save_data(data)
        tab.toast("Auto-follow", "Enabled" if is_auto_follow else "Disabled", "SUCCESS")

    async def handle_follow_target_input(value):
        nonlocal follow_target_id, is_auto_follow
        follow_target_id = str(value or "").strip()
        data = load_data()
        if is_auto_follow and not follow_target_id.isdigit():
            is_auto_follow = False
            ui_refs["follow_toggle"].checked = False
            data["settings"]["auto_follow"] = False
            tab.toast("Auto-follow disabled", "Enter a numeric Discord user ID.", "INFO")
        if data["settings"].get("follow_target_id") != follow_target_id:
            data["settings"]["follow_target_id"] = follow_target_id
        save_data(data)

    async def handle_apply_timer():
        apply_timer_btn.loading = True
        try:
            val = pending_timer_minutes
            data = load_data()
            data["settings"]["auto_disconnect_minutes"] = val if val > 0 else None
            save_data(data)
            await schedule_disconnect(val)
            tab.toast("Disconnect timer", f"Set to {val} min" if val > 0 else "Disabled", "SUCCESS")
        except Exception as e:
            tab.toast("Timer error", str(e), "ERROR")
        finally:
            apply_timer_btn.loading = False

    async def update_command_setting(setting_key, checked, label):
        command_settings = get_slash_response_settings()
        command_settings[setting_key] = as_bool(checked)
        save_slash_response_settings(command_settings)
        sync_command_settings_ui(command_settings)
        state = "enabled" if command_settings[setting_key] else "disabled"
        tab.toast("Command Settings", f"{label} {state}", "SUCCESS")

    async def handle_command_ephemeral_toggle(checked):
        await update_command_setting("ephemeral", checked, "Ephemeral responses")

    async def handle_command_theme_toggle(checked):
        await update_command_setting("use_theme", checked, "Nighty V2 theme")

    async def handle_command_detailed_toggle(checked):
        await update_command_setting("detailed_status", checked, "Detailed status")

    def handle_open_channel_info():
        cur = get_current_voice_channel()
        if not cur:
            tab.toast("Not connected", "Connect through VC Manager to view channel details.", "INFO")
            return

        tab.channelinfomodal(
            channel_name=html_escape(str(cur.name)),
            guild_name=html_escape(str(cur.guild.name)),
            channel_id=str(cur.id),
            bitrate=f"{int(getattr(cur, 'bitrate', 64000) / 1000)} kbps",
            user_limit=str(cur.user_limit) if getattr(cur, 'user_limit', 0) > 0 else "Unlimited",
            rtc_region=str(getattr(cur, 'rtc_region', 'Automatic') or 'Automatic').title(),
            participants_count=str(len(cur.members)),
            onClose=lambda: None
        )

    def handle_open_history():
        data = load_data()
        sessions = get_primary_session_history(include_live=True)
        live_seconds = (
            (datetime.now() - connection_start_time).total_seconds()
            if connection_start_time and is_actually_connected() else 0
        )
        total_time = format_duration(data.get("total_vc_time_seconds", 0) + live_seconds)

        def clear_sessions():
            current_data = load_data()
            current_data["sessions"] = []
            save_data(current_data)
            tab.toast("History cleared", "VC Manager removed the saved sessions.", "SUCCESS")

        tab.sessionhistorymodal(
            sessions_json=json.dumps(json.dumps(sessions, ensure_ascii=False)),
            total_vc_time=total_time,
            onClear=clear_sessions,
            onClose=lambda: None
        )

    async def apply_channel_status(status, clear=False):
        button = clear_channel_status_btn if clear else set_channel_status_btn
        button.loading = True
        try:
            cur = get_current_voice_channel()
            if not cur:
                tab.toast("Error", "Connect through VC Manager first", "ERROR")
                return
            if not can_set_voice_channel_status(cur):
                tab.toast("Permission denied", "You cannot set this voice channel's status", "ERROR")
                update_all_ui()
                return

            success, result = await set_voice_channel_status(cur, status)
            if success:
                ui_refs["voice_status_input"].value = result
                ui_refs["voice_status_hint"].content = (
                    "VC Manager cleared the channel status."
                    if not result
                    else "VC Manager updated the channel status."
                )
                tab.toast(
                    "Channel Status",
                    "Cleared" if not result else "Updated",
                    "SUCCESS"
                )
            else:
                tab.toast("Channel Status", result, "ERROR")
        except Exception as e:
            print(f"[VC Manager] Could not apply voice channel status: {e}", type_="ERROR")
            tab.toast("Channel Status", "Nighty could not update the channel status.", "ERROR")
        finally:
            button.loading = False

    async def handle_set_channel_status():
        await apply_channel_status(ui_refs["voice_status_input"].value or "")

    async def handle_clear_channel_status():
        await apply_channel_status("", clear=True)

    def form_input_text(value):
        if isinstance(value, dict):
            value = value.get("value", "")
        elif isinstance(value, (list, tuple)):
            value = value[0] if value else ""
        return str(value or "")

    def handle_token_input(value):
        token_form_state["token"] = form_input_text(value)

    def handle_token_name_input(value):
        token_form_state["name"] = form_input_text(value)

    async def handle_add_token():
        add_token_btn.loading = True
        try:
            # Yield once so mobile input callbacks queued immediately before the
            # tap can update token_form_state before this handler reads it.
            await asyncio.sleep(0.12)
            token_value = token_form_state.get("token") or form_input_text(ui_refs["token_input"].value)
            name_value = token_form_state.get("name") or form_input_text(ui_refs["token_name_input"].value)
            tok = token_value.strip().strip('"').strip("'").strip()
            name = name_value.strip() or f"Token #{len(token_clients)+1}"

            if not tok:
                tab.toast("Token missing", "Paste the account token before pressing Add Account.", "ERROR")
                return
            if any(character.isspace() for character in tok):
                tab.toast("Token has spaces", "Copy the raw token only. Remove quotes, labels, and spaces.", "ERROR")
                return

            data = load_tokens()
            if data.get("_load_error"):
                tab.toast("Token file is broken", data["_load_error"], "ERROR")
                return
            if any(isinstance(item, dict) and str(item.get("token", "")).strip() == tok for item in data.get("tokens", [])):
                tab.toast("Account already added", "Managed Accounts already contains this token.", "ERROR")
                return
            new_id = str(uuid.uuid4())
            data["tokens"].append({
                "id": new_id,
                "token": tok,
                "name": name,
                "preferred_guild_id": "",
                "preferred_channel_id": "",
                "stay_connected": False,
            })
            if not save_tokens(data):
                tab.toast("Could not save account", f"Nighty could not write {TOKENS_DATA_PATH.name}. Check folder permissions and free disk space.", "ERROR")
                return

            client = GatewayVoiceClient(tok, name=name, record_id=new_id)
            token_clients[new_id] = client
            started = await client.start_gateway()
            if started:
                for _ in range(120):
                    if client.user_id and client.ws and not client.ws.closed:
                        break
                    if client.status == "Token rejected" or (client.ws and client.ws.closed):
                        break
                    await asyncio.sleep(0.1)
                if not client.user_id and not client.last_error:
                    client._set_status(
                        "Login timed out",
                        "Discord did not finish signing this account in. Check the token, account verification, VPN, and firewall."
                    )

            ui_refs["token_input"].value = ""
            ui_refs["token_name_input"].value = ""
            token_form_state["token"] = ""
            token_form_state["name"] = ""
            refresh_destination_account_items(preserve_selection=True)
            update_token_table_rows()
            if client.user_id:
                tab.toast("Account added", f"{name} signed in and can join voice channels.", "SUCCESS")
            else:
                tab.toast(
                    "Account saved, login failed",
                    client.last_error or "Discord did not accept the account session. Remove it and add a fresh token.",
                    "ERROR"
                )
        except Exception as e:
            print(f"[VC Manager] Could not add managed account: {e}", type_="ERROR")
            tab.toast("Account could not be added", f"VC Manager stopped before the account was ready: {type(e).__name__}. Check the Nighty console for the full error.", "ERROR")
        finally:
            add_token_btn.loading = False

    async def handle_conn_all_tokens():
        conn_all_btn.loading = True
        try:
            clients = list(token_clients.items())
            if not clients:
                tab.toast("No managed accounts", "Add at least one account before pressing Join All.", "ERROR")
                return

            join_jobs = []
            join_clients = []
            failures = []
            for token_id, client in clients:
                guild_id, channel_id, destination_source = resolve_token_join_destination(token_id)
                if not guild_id or not channel_id:
                    failures.append((
                        client.name,
                        "Choose this account's server and voice channel in Account Destination. "
                        "No active or saved managed destination is available.",
                    ))
                    continue
                print(
                    f"[VC Manager] Join All selected guild {guild_id}, channel {channel_id} "
                    f"for {client.name} (record {str(token_id)[:8]}) using {destination_source}.",
                    type_="INFO",
                )
                join_clients.append(client)
                join_jobs.append(client.join_vc(
                    guild_id,
                    channel_id,
                    mute=bulk_voice_state["mute"],
                    deaf=bulk_voice_state["deaf"],
                    stream=bulk_voice_state["stream"],
                    video=bulk_voice_state["video"],
                ))

            results = await asyncio.gather(*join_jobs, return_exceptions=True) if join_jobs else []
            connected = 0
            save_warnings = []
            for client, result in zip(join_clients, results):
                if result is True:
                    connected += 1
                    if client.last_error:
                        save_warnings.append((client.name, client.last_error))
                    continue
                if isinstance(result, Exception):
                    print(f"[VC Manager] Join All failed for {client.name}: {result}", type_="ERROR")
                    client._set_status("Join failed", f"VC Manager stopped while joining this account: {type(result).__name__}. Check the Nighty console for the full error.")
                failures.append((client.name, client.last_error or "Discord did not confirm the voice join."))

            update_token_table_rows()
            if connected == len(clients) and save_warnings:
                warning_name, warning_text = save_warnings[0]
                tab.toast("Accounts connected, destination not saved", f"{warning_name}: {warning_text}", "ERROR")
            elif connected == len(clients):
                tab.toast("Accounts connected", f"All {connected} managed accounts joined their saved destinations.", "SUCCESS")
            else:
                first_name, first_error = failures[0]
                remaining = len(failures) - 1
                suffix = f" Check Managed Accounts for {remaining} more error{'s' if remaining != 1 else ''}." if remaining else ""
                if connected:
                    tab.toast(
                        "Some accounts did not join",
                        f"{connected} of {len(clients)} joined. {first_name}: {first_error}{suffix}",
                        "ERROR"
                    )
                else:
                    tab.toast(
                        "No accounts joined",
                        f"{first_name}: {first_error}{suffix}",
                        "ERROR"
                    )
        except Exception as e:
            print(f"[VC Manager] Join All stopped: {e}", type_="ERROR")
            tab.toast("Join All stopped", f"VC Manager could not finish the request: {type(e).__name__}. No saved accounts were removed.", "ERROR")
        finally:
            conn_all_btn.loading = False

    async def handle_apply_all_controls():
        apply_all_controls_btn.loading = True
        try:
            connected_clients = [client for client in token_clients.values() if client.is_in_vc]
            if not connected_clients:
                tab.toast("No connected accounts", "Join at least one managed account before applying controls.", "ERROR")
                return
            requested = dict(bulk_voice_state)
            results = await asyncio.gather(
                *(client.set_voice_controls(**requested) for client in connected_clients),
                return_exceptions=True,
            )
            updated = sum(result is True for result in results)
            update_token_table_rows()
            if updated == len(connected_clients):
                tab.toast("Controls applied", f"Updated {updated} connected account{'s' if updated != 1 else ''}.", "SUCCESS")
            else:
                failed_client = next(
                    (
                        client for client, result in zip(connected_clients, results)
                        if result is not True
                    ),
                    None,
                )
                failure_text = (
                    failed_client.last_error
                    if failed_client and failed_client.last_error
                    else "Discord rejected one or more voice-control updates."
                )
                tab.toast(
                    "Some controls were not applied",
                    f"Updated {updated} of {len(connected_clients)} accounts. {failure_text}",
                    "ERROR",
                )
        except Exception as error:
            print(f"[VC Manager] Apply Controls stopped: {error}", type_="ERROR")
            tab.toast("Apply Controls stopped", f"VC Manager could not finish the request: {type(error).__name__}.", "ERROR")
        finally:
            apply_all_controls_btn.loading = False

    async def handle_disc_all_tokens():
        disc_all_btn.loading = True
        try:
            connected_clients = [client for client in token_clients.values() if client.is_in_vc]
            if not connected_clients:
                tab.toast("Nothing to disconnect", "No managed account is connected to a voice channel.", "ERROR")
                return
            results = await asyncio.gather(
                *(client.leave_vc() for client in connected_clients),
                return_exceptions=True
            )
            count = sum(result is True for result in results)

            update_token_table_rows()
            if count == len(connected_clients):
                tab.toast("Accounts disconnected", f"Disconnected {count} managed account{'s' if count != 1 else ''}.", "SUCCESS")
            else:
                tab.toast("Some accounts stayed connected", f"Disconnected {count} of {len(connected_clients)} accounts. Check Managed Accounts for errors.", "ERROR")
        except Exception as e:
            print(f"[VC Manager] Leave All stopped: {e}", type_="ERROR")
            tab.toast("Leave All stopped", f"VC Manager could not finish the request: {type(e).__name__}. Check Managed Accounts to see which accounts are still connected.", "ERROR")
        finally:
            disc_all_btn.loading = False

    def handle_token_history(row_id):
        row_id_str = str(row_id or "")
        saved_item = get_saved_token_item(row_id_str) or {}
        client = token_clients.get(row_id_str)
        sessions = [
            dict(session) for session in saved_item.get("sessions", [])
            if isinstance(session, dict)
        ]
        try:
            total_seconds = max(0.0, float(saved_item.get("total_vc_time_seconds", 0) or 0))
        except (TypeError, ValueError):
            total_seconds = 0.0
        if client and client.voice_session_started_at:
            live_seconds = max(0.0, (datetime.now() - client.voice_session_started_at).total_seconds())
            guild_name, channel_name = client._voice_target_names(
                client.voice_session_guild_id,
                client.voice_session_channel_id,
            )
            sessions.append({
                "guild_id": client.voice_session_guild_id,
                "guild_name": guild_name,
                "channel_id": client.voice_session_channel_id,
                "channel_name": channel_name,
                "start_time": client.voice_session_started_at.isoformat(),
                "duration_seconds": live_seconds,
                "reason": "Active",
            })
            total_seconds += live_seconds

        account_name = str(
            (client.name if client else None)
            or saved_item.get("label")
            or saved_item.get("name")
            or "Managed Account"
        )

        def clear_token_sessions():
            if update_saved_token(row_id_str, sessions=[], total_vc_time_seconds=0.0):
                tab.toast("Session logs cleared", f"Cleared saved sessions for {account_name}.", "SUCCESS")
            else:
                tab.toast("Could not clear logs", "Nighty could not update the managed-account data file.", "ERROR")

        tab.sessionhistorymodal(
            title=f"{html_escape(account_name)} Sessions",
            sessions_json=json.dumps(json.dumps(sessions, ensure_ascii=False)),
            total_vc_time=format_duration(total_seconds),
            onClear=clear_token_sessions,
            onClose=lambda: None,
        )

    def handle_token_action(payload):
        try:
            action_data = json.loads(payload) if isinstance(payload, str) else payload
        except (TypeError, ValueError, json.JSONDecodeError):
            action_data = {"id": str(payload), "action": "account"}
        if not isinstance(action_data, dict):
            action_data = {"id": str(payload), "action": "account"}

        row_id = str(action_data.get("id") or "")
        action = str(action_data.get("action") or "account").lower()
        if not row_id:
            print("[VC Manager] Managed Accounts sent an action without an account ID.", type_="ERROR")
            tab.toast("Account action failed", "Refresh the VC Manager page and try again.", "ERROR")
            return
        print(
            f"[VC Manager] Managed account action '{action}' received for record {row_id[:8]}.",
            type_="INFO",
        )
        if action == "account":
            handle_token_connection(row_id, "connection")
            return
        if action in {"join", "leave", "connection"}:
            handle_token_connection(row_id, action)
            return
        if action == "remove":
            handle_token_remove(row_id)
            return
        if action == "logs":
            handle_token_history(row_id)
            return

        control_map = {
            "mute": ("is_muted", "mute", "Mute"),
            "deaf": ("is_deafened", "deaf", "Deafen"),
            "stream": ("is_streaming", "stream", "Stream"),
            "video": ("is_camera_on", "video", "Camera"),
        }
        control = control_map.get(action)
        if not control:
            tab.toast("Unknown control", "Refresh the VC Manager page and try again.", "ERROR")
            return

        async def update_token_control():
            client = token_clients.get(row_id)
            if not client or not client.is_in_vc:
                tab.toast(
                    "Account is not connected",
                    "Join this managed account to a voice channel first.",
                    "ERROR",
                )
                update_token_table_rows()
                return

            attribute, argument, label = control
            enabled = not bool(getattr(client, attribute, False))
            result = await client.set_voice_controls(**{argument: enabled})
            update_token_table_rows()
            if result:
                state_text = "enabled" if enabled else "disabled"
                tab.toast(
                    f"{label} {state_text}",
                    f"{client.name} now uses this setting.",
                    "SUCCESS",
                )
            else:
                tab.toast(
                    f"Could not update {label.lower()}",
                    client.last_error or "Discord rejected the voice control update.",
                    "ERROR",
                )

        safe_create_task(update_token_control())

    def handle_token_connection(row_id, requested_action="connection"):
        row_id_str = str(row_id)
        requested_action = str(requested_action or "connection").lower()

        async def update_connection():
            client = token_clients.get(row_id_str)
            try:
                if not client:
                    print(
                        f"[VC Manager] Managed account action used an unknown record ID: {row_id_str}",
                        type_="ERROR",
                    )
                    tab.toast(
                        "Account is not ready",
                        "Refresh Managed Accounts. If it still does not load, remove it and add a fresh token.",
                        "ERROR",
                    )
                    return

                should_leave = requested_action == "leave" or (
                    requested_action == "connection" and client.is_in_vc
                )
                if should_leave:
                    if not client.is_in_vc:
                        tab.toast("Account is not connected", f"{client.name} is already out of voice.", "ERROR")
                        return
                    if await client.leave_vc():
                        tab.toast(
                            "Account disconnected",
                            f"{client.name} left the voice channel and remains saved.",
                            "SUCCESS",
                        )
                    else:
                        tab.toast(
                            "Disconnect failed",
                            client.last_error or "Discord did not accept the leave request.",
                            "ERROR",
                        )
                    return

                if client.is_in_vc:
                    tab.toast("Account is already connected", f"{client.name} is already in voice.", "ERROR")
                    return

                target_guild_id, target_channel_id, destination_source = resolve_token_join_destination(row_id_str)
                if not target_guild_id or not target_channel_id:
                    client._set_status(
                        "Destination required",
                        "Choose this account's server and voice channel in Account Destination. "
                        "No active or saved managed destination is available.",
                    )
                    tab.toast("Destination required", client.last_error, "ERROR")
                    return

                print(
                    f"[VC Manager] Joining {client.name} (record {row_id_str[:8]}) "
                    f"to guild {target_guild_id}, channel {target_channel_id} using {destination_source}.",
                    type_="INFO",
                )
                joined = await client.join_vc(
                    target_guild_id,
                    target_channel_id,
                    mute=is_muted,
                    deaf=is_deafened,
                    stream=is_streaming,
                    video=is_camera_on,
                )
                if joined:
                    channel_name = target_channel_id
                    for channel_item in client.channel_cache.get(target_guild_id, []):
                        if str(channel_item.get("id")) == target_channel_id:
                            channel_name = channel_item.get("name") or target_channel_id
                            break
                    print(
                        f"[VC Manager] {client.name} joined guild {target_guild_id}, channel {target_channel_id}.",
                        type_="INFO",
                    )
                    if client.last_error:
                        tab.toast("Account connected, destination not saved", client.last_error, "ERROR")
                    else:
                        tab.toast(
                            "Account connected",
                            f"{client.name} joined #{channel_name} with the current voice controls.",
                            "SUCCESS",
                        )
                else:
                    error_text = client.last_error or "Discord did not confirm the voice join."
                    print(
                        f"[VC Manager] Join failed for {client.name} (record {row_id_str[:8]}): {error_text}",
                        type_="ERROR",
                    )
                    tab.toast("Account could not join", error_text, "ERROR")
            except Exception as error:
                account_name = client.name if client else f"record {row_id_str[:8]}"
                print(
                    f"[VC Manager] Join action crashed for {account_name}: "
                    f"{type(error).__name__}: {error}",
                    type_="ERROR",
                )
                if client:
                    client._set_status(
                        "Join failed",
                        f"VC Manager hit {type(error).__name__} while joining this account. "
                        "Check the Nighty console for the technical error.",
                    )
                tab.toast(
                    "Account could not join",
                    f"VC Manager hit {type(error).__name__}. Check the Nighty console for the full error.",
                    "ERROR",
                )
            finally:
                update_token_table_rows()

        safe_create_task(update_connection())

    def handle_token_remove(row_id):
        row_id_str = str(row_id)

        async def remove_token():
            data = load_tokens()
            if data.get("_load_error"):
                tab.toast("Token file is broken", data["_load_error"], "ERROR")
                return
            original_count = len(data.get("tokens", []))
            data["tokens"] = [
                item for item in data.get("tokens", [])
                if not isinstance(item, dict) or str(item.get("id") or "") != row_id_str
            ]
            if len(data["tokens"]) == original_count:
                tab.toast("Account not found", "Refresh Managed Accounts and try again.", "ERROR")
                return
            if not save_tokens(data):
                tab.toast("Could not remove account", "Nighty could not update the token file.", "ERROR")
                return
            client = token_clients.pop(row_id_str, None)
            if client:
                await client.close()
            refresh_destination_account_items(preserve_selection=True)
            update_token_table_rows()
            tab.toast("Account removed", "VC Manager removed the saved token.", "SUCCESS")

        safe_create_task(remove_token())

    def handle_token_leave(row_id):
        handle_token_connection(str(row_id), "leave")

    # Wire all callbacks
    server_select.onChange = handle_server_select
    channel_select.onChange = handle_channel_select
    destination_account_select.onChange = handle_destination_account_select
    destination_server_select.onChange = handle_destination_server_select
    destination_channel_select.onChange = handle_destination_channel_select
    destination_join_button.onClick = handle_destination_join
    destination_refresh_button.onClick = handle_destination_refresh
    join_button.onClick = handle_join
    leave_button.onClick = handle_leave
    refresh_list_button.onClick = handle_refresh_list
    mute_toggle.onChange = handle_mute_toggle
    deafen_toggle.onChange = handle_deafen_toggle
    stream_toggle.onChange = handle_stream_toggle
    camera_toggle.onChange = handle_camera_toggle
    reconnect_toggle.onChange = handle_reconnect_toggle
    leave_alone_toggle.onChange = handle_leave_alone_toggle
    follow_toggle.onChange = handle_follow_toggle
    follow_target_input.onInput = handle_follow_target_input
    token_input.onInput = handle_token_input
    token_name_input.onInput = handle_token_name_input
    apply_timer_btn.onClick = handle_apply_timer
    command_ephemeral_toggle.onChange = handle_command_ephemeral_toggle
    command_theme_toggle.onChange = handle_command_theme_toggle
    command_detailed_toggle.onChange = handle_command_detailed_toggle
    details_btn.onClick = handle_open_channel_info
    logs_btn.onClick = handle_open_history
    set_channel_status_btn.onClick = handle_set_channel_status
    clear_channel_status_btn.onClick = handle_clear_channel_status
    add_token_btn.onClick = handle_add_token
    bulk_mute_toggle.onChange = handle_bulk_mute_toggle
    bulk_deafen_toggle.onChange = handle_bulk_deafen_toggle
    bulk_stream_toggle.onChange = handle_bulk_stream_toggle
    bulk_camera_toggle.onChange = handle_bulk_camera_toggle
    conn_all_btn.onClick = handle_conn_all_tokens
    apply_all_controls_btn.onClick = handle_apply_all_controls
    disc_all_btn.onClick = handle_disc_all_tokens

    # ==================== STARTUP SEQUENCE ====================

    # 1. Apply saved user settings
    data = load_data()
    settings = data.get("settings", {})

    is_muted = as_bool(settings.get("muted", False))
    is_deafened = as_bool(settings.get("deafened", False))
    is_streaming = as_bool(settings.get("streaming", False))
    is_camera_on = as_bool(settings.get("camera_on", False))
    is_auto_reconnect = as_bool(settings.get("auto_reconnect", False))
    is_leave_when_alone = as_bool(settings.get("leave_when_alone", False))
    is_auto_follow = as_bool(settings.get("auto_follow", False))
    follow_target_id = str(settings.get("follow_target_id", ""))
    if is_auto_follow and not follow_target_id.isdigit():
        is_auto_follow = False

    mute_toggle.checked = is_muted
    deafen_toggle.checked = is_deafened
    stream_toggle.checked = is_streaming
    camera_toggle.checked = is_camera_on
    bulk_mute_toggle.checked = is_muted
    bulk_deafen_toggle.checked = is_deafened
    bulk_stream_toggle.checked = is_streaming
    bulk_camera_toggle.checked = is_camera_on
    bulk_voice_state.update({
        "mute": is_muted,
        "deaf": is_deafened,
        "stream": is_streaming,
        "video": is_camera_on,
    })
    reconnect_toggle.checked = is_auto_reconnect
    leave_alone_toggle.checked = is_leave_when_alone
    follow_toggle.checked = is_auto_follow
    follow_target_input.value = follow_target_id
    sync_command_settings_ui()

    saved_timer = timer_minutes(settings.get("auto_disconnect_minutes"))
    if saved_timer and is_actually_connected():
        safe_create_task(schedule_disconnect(saved_timer))

    # 2. Init secondary tokens
    init_token_clients()
    update_token_table_rows()

    # 3. Fast server & channel pre-population
    def version_tuple(value):
        parts = [int(part) for part in re.findall(r"\d+", str(value or ""))[:3]]
        return tuple((parts + [0, 0, 0])[:3])

    async def check_for_script_update():
        await asyncio.sleep(1.0)
        timeout = aiohttp.ClientTimeout(total=12)
        headers = {"User-Agent": f"Nighty-VC-Manager/{SCRIPT_VERSION}"}
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(VERSION_SOURCE_URL) as response:
                    if response.status != 200:
                        print(
                            f"[VC Manager] Update check returned HTTP {response.status}. The script will keep running.",
                            type_="INFO",
                        )
                        return
                    source = await response.text()
            match = re.search(r'^\s*SCRIPT_VERSION\s*=\s*["\']([^"\']+)["\']', source, re.MULTILINE)
            if not match:
                match = re.search(r'name\s*=\s*["\']VC Manager v([^"\']+)["\']', source)
            if not match:
                print("[VC Manager] The update check could not read the repository version.", type_="INFO")
                return
            latest_version = match.group(1).strip()
            if version_tuple(latest_version) <= version_tuple(SCRIPT_VERSION):
                return
            tab.updateavailablemodal(
                current_version=SCRIPT_VERSION,
                latest_version=latest_version,
                project_url=PROJECT_URL,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(
                f"[VC Manager] Update check failed: {type(error).__name__}: {error}. The script will keep running.",
                type_="INFO",
            )

    async def startup_sync():
        await asyncio.sleep(0.02)
        # Paint the primary connection and participants before secondary gateways
        # finish logging in.
        await sync_voice_state(update_ui=True)
        await refresh_server_list(preserve_selection=True)
        if token_clients:
            clients = list(token_clients.values())
            results = await asyncio.gather(
                *(client.start_gateway() for client in clients),
                return_exceptions=True
            )
            for client, result in zip(clients, results):
                if isinstance(result, Exception):
                    print(f"[VC Manager] Could not start {client.name}: {result}", type_="ERROR")
            update_token_table_rows()
        refresh_destination_account_items(preserve_selection=True)
    async def live_timer_ticker():
        tick_count = 0
        while True:
            try:
                await asyncio.sleep(1)
                tick_count += 1
                cur = get_current_voice_channel()
                if cur and connection_start_time:
                    session_seconds = (datetime.now() - connection_start_time).total_seconds()
                    total_seconds = load_data().get("total_vc_time_seconds", 0) + session_seconds
                    ui_refs["session_time_text"].content = f"Session: {format_duration(session_seconds)}"
                    ui_refs["total_time_text"].content = f"Total Time: {format_duration(total_seconds)}"
                if tick_count % 5 == 0:
                    await sync_voice_state(update_ui=True)
                if tick_count % 10 == 0:
                    # Some Nighty web builds redraw the tab after Socket.IO connects.
                    # Reinstall CUI so refreshes do not leave serialized placeholders.
                    reeval()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[VC Manager] Timer update failed: {e}", type_="ERROR")

    def ensure_live_timer():
        existing = runtime_state.get("ticker_task")
        if existing and not existing.done():
            return existing
        runtime_state["ticker_task"] = safe_create_task(live_timer_ticker())
        return runtime_state["ticker_task"]

    @bot.listen("on_ready")
    async def handle_bot_ready():
        await startup_sync()
        ensure_live_timer()

    runtime_state["listeners"].append(("on_ready", handle_bot_ready))

    def schedule_browser_ui_restore():
        previous_restore = runtime_state.get("ui_restore_task")
        if previous_restore and not previous_restore.done():
            previous_restore.cancel()

        async def restore_after_render():
            # Nighty paints the private tab in stages. Run CUI after each paint.
            for delay in (0.10, 0.35, 0.75, 1.50, 3.00):
                await asyncio.sleep(delay)
                reeval()
                evaluate_js(VC_UI_JS)

        runtime_state["ui_restore_task"] = safe_create_task(restore_after_render())
        return runtime_state["ui_restore_task"]

    initial_connected = is_actually_connected()
    for toggle in (mute_toggle, deafen_toggle, stream_toggle, camera_toggle):
        toggle.disabled = not initial_connected

    @socketio.on("connect")
    def vc_manager_ui_connect():
        """Publish the current UI tree after the private tab reconnects."""
        tab.render()
        evaluate_js(VC_UI_JS)
        update_all_ui()
        schedule_browser_ui_restore()

    vc_manager_ui_connect._vc_manager_socket_handler = True

    # 4. Render Nighty tab, then install script-specific DOM helpers.
    tab.render()
    evaluate_js(VC_UI_JS)
    update_all_ui()
    automation_pager.build()
    update_token_table_rows()
    schedule_browser_ui_restore()

    bot_is_ready = getattr(bot, "is_ready", None)
    if not callable(bot_is_ready) or bot_is_ready():
        safe_create_task(startup_sync())
    runtime_state["update_check_task"] = safe_create_task(check_for_script_update())
    ensure_live_timer()

vc_manager_farm_script()
