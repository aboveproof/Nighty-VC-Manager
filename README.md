<div align="center">

# Nighty VC Manager

Voice controls, session tracking, automation, slash commands, and multi-account connections for Nighty.

[![Version](https://img.shields.io/badge/version-4.0-5865F2?style=flat-square)](./VCManagerV4.py)
[![NightyScript](https://img.shields.io/badge/NightyScript-4.9-111827?style=flat-square)](https://nighty.one/)
[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-22c55e?style=flat-square)](https://github.com/aboveproof/Nighty-VC-Manager/blob/main/LICENSE)

</div>

## About

Nighty VC Manager adds a dedicated voice dashboard to Nighty and a `/vc` slash-command group to Discord. It tracks only the voice connection opened through the script, so your normal Discord voice session does not take over the manager.

The dashboard handles the primary Nighty account and optional secondary accounts. It also records session time, shows channel participants, edits voice-channel status when your account has permission, and provides reconnect and disconnect automation.

> [!WARNING]
> Nighty is a Discord selfbot. Selfbots and user-account automation can violate Discord's Terms of Service and may put accounts at risk. This project is not affiliated with Discord or Nighty. You are responsible for how you use it.

## Features

### Voice connection

- Select a server and voice channel, or enter a channel ID directly.
- Join and disconnect without using a text command.
- Keep the managed connection separate from voice sessions opened outside the script.
- Refresh cached servers and channels without reloading Nighty.
- View the current server, channel, channel ID, session length, and total tracked time.

### Voice controls

- Microphone mute
- Audio deafen
- Screen-share state
- Camera state
- Explicit `enable` and `disable` choices in slash commands
- Current-state checks that prevent duplicate updates

The stream and camera controls update Discord voice-state flags. Actual media behavior still depends on Discord and the Nighty build in use.

### Channel information

- Fixed-height participant list with up to five users per page
- Compact participant IDs
- Active voice flags shown as `Mute | Deaf | Stream | Camera`
- Channel telemetry for bitrate, region, user limit, occupancy, and channel ID
- Voice-channel status editor with permission checks
- Session history with a clear-history action

### Automation

- Reconnect after an unexpected drop
- Leave when the managed account is alone
- Follow a user between voice channels by user ID
- Disconnect timer from 0 to 120 minutes
- Saved settings that survive script reloads

### Multi-account connections

- Add secondary Discord accounts by token and label
- Join or leave all managed accounts together
- Join or leave one managed account directly from its table row
- Mirror the primary mute, deafen, stream, and camera controls to connected accounts
- Override mute, deafen, stream, or camera for one account from its table row
- View each account's channel and connection state
- Paginated managed-account list
- Remove individual accounts from the manager

Secondary accounts use direct Discord Gateway connections. Tokens stay in a local JSON file and are not encrypted.

### Slash commands and Components V2

- Real application commands through the Nighty Slash API approach
- Commands registered for servers, bot DMs, and user-installed private or group DMs
- Owner-only command execution
- Discord Components V2 responses
- Active Nighty V2 theme support
- Ephemeral responses enabled by default
- Automatic command fingerprinting and synchronization

## Requirements

- A working [Nighty](https://nighty.one/) installation
- A Nighty build that exposes CUI and `bot_app` for slash commands
- Python 3
- `aiohttp`, normally included with Nighty

If Nighty does not include `aiohttp`, install it in the Python environment used by Nighty:

```bash
pip install aiohttp
```

## Language and architecture

VC Manager is written in **Python for NightyScript 4.9**. NightyScript is Nighty's embedded scripting environment, not a standalone Discord bot framework. Nighty owns the logged-in Discord client and injects its scripting API into the file when the script loads.

That distinction matters if you plan to read, modify, or contribute to the code:

- `@nightyScript(...)` declares the script and its metadata. The decorated entry function is called at the bottom of the file to register the UI, listeners, commands, and background tasks.
- `bot`, `bot_app`, `UI`, `Tab`, `getConfigData`, `updateConfigData`, and `getScriptsPath` come from Nighty's runtime. They are available without imports.
- The script must not import `discord`, `nighty`, or symbols from either package. Nighty uses a modified `discord.py-self` environment and provides supported objects itself.
- Discord and Nighty operations are asynchronous. Connection work, API requests, UI callbacks, event listeners, and cleanup run through `async` functions and tasks.
- The dashboard uses CUI. Python classes describe elements with HTML templates, while Nighty connects their callbacks to the embedded web interface.
- Slash commands use Nighty's `bot_app` client and Discord application-command endpoints. Responses use Discord Components V2 where available instead of `discord.Embed`, which NightyScript does not support.
- Persistent settings use Nighty's configuration store for simple values and JSON files under `getScriptsPath()` for session history and managed accounts.
- Secondary accounts do not become additional Nighty clients. Each one opens its own authenticated Discord Gateway and voice connection managed by this script.

The main runtime path is:

```text
Nighty loads VCManagerV4.py
  -> @nightyScript initializes the script
  -> CUI registers the VC Manager tab
  -> bot_app synchronizes /vc commands
  -> a dashboard action or slash command updates managed state
  -> Discord Gateway and voice WebSocket tasks apply the change
  -> JSON storage records settings and session history
```

## Installation

1. Download [`VCManagerV4.py`](./VCManagerV4.py).
2. Import the file through Nighty's script manager, or place it in the Nighty scripts directory.
3. Enable the script.
4. Open the **VC Manager** tab in Nighty.
5. Select a server and voice channel, then press **Join Voice**.

The script registers `/vc` after Nighty finishes connecting. If Discord does not show the command after the first load, wait a few seconds and press `Ctrl+R` in Discord.

## Slash commands

| Command | Arguments | Action |
| --- | --- | --- |
| `/vc join` | `channel` | Join by channel ID or part of a channel name. |
| `/vc load` | `channel_id` | Join with mute, deafen, stream, and camera enabled. |
| `/vc leave` | None | Disconnect the managed voice session. |
| `/vc mute` | `state: enable/disable` | Set microphone mute. |
| `/vc deafen` | `state: enable/disable` | Set audio deafen. |
| `/vc stream` | `state: enable/disable` | Set screen-share state. |
| `/vc camera` | `state: enable/disable` | Set camera state. |
| `/vc list` | None | List users in the managed voice channel. |
| `/vc history` | None | Show the five most recent tracked sessions. |
| `/vc status` | None | Show the managed connection state. |
| `/vc stats` | None | Show total voice time, session statistics, and managed-account usage. |
| `/vc settings` | Optional booleans | View or change slash-response settings. |
| `/vc help` | None | Show the command list. |

### Slash response settings

`/vc settings` accepts three optional values:

| Option | Default | Effect |
| --- | --- | --- |
| `ephemeral` | `true` | Shows responses only to you. |
| `theme` | `true` | Applies the active Nighty V2 theme to Components V2 responses. |
| `detailed` | `true` | Includes server, channel ID, and session time in `/vc status`. |

Run `/vc settings` without arguments to view the saved values.

## Local data

VC Manager creates these files under Nighty's `json` script-data directory:

```text
vc_manager_data.json      Session history, timers, automation, and slash settings
vc_manager_tokens.json    Secondary account tokens and labels
slashapi_state.json       Slash-command definition fingerprint
```

Do not commit or share these files. `vc_manager_tokens.json` contains usable Discord account tokens in plain text.

## Behavior and limits

- VC Manager tracks sessions opened through VC Manager. A normal Discord voice connection does not become the managed session.
- One Discord account can hold one voice state per server. Use a secondary account for two channels in the same server.
- Channel-status editing requires Discord's **Voice Channel Status** permission for the connected channel.
- The script depends on private Nighty and Discord behavior. Client or API updates can break individual controls.
- Slash commands require Nighty's embedded application-command client. The CUI dashboard can still load when that client is unavailable.
- Global slash-command changes can take time to appear in Discord's cache.

## Troubleshooting

<details>
<summary><strong>The /vc command does not appear</strong></summary>

Wait for the Nighty console to report a successful slash sync, then reload Discord with `Ctrl+R`. Restart Nighty if an old script generation still owns the command. Confirm that your Nighty build exposes `bot_app`.

</details>

<details>
<summary><strong>The dashboard shows stale information after replacing the script</strong></summary>

Reload Nighty's private tab. Restart Nighty if the previous script generation left gateway tasks running.

</details>

<details>
<summary><strong>Screen share or camera does not start</strong></summary>

The controls send Discord voice-state flags. Discord may reject the update, and the script does not capture or transmit video. Check the Nighty console for the returned error.

</details>

<details>
<summary><strong>Channel status is disabled</strong></summary>

Connect through VC Manager first. The account also needs the Voice Channel Status permission in that channel.

</details>

<details>
<summary><strong>A managed account will not connect</strong></summary>

Check the token, account access to the server, and channel permissions. Remove and add the account again if its token changed.

</details>

## Security

- Treat every Discord token as a password.
- Keep Nighty's script-data directory private.
- Never post `vc_manager_tokens.json` in an issue or support channel.
- Use secondary accounts at your own risk.
- Review the script before loading updates from a fork.

## Credits

- **@rico** for the project and primary development
- **@snowy** and **@factz** for script development
- [vahrervert/cui](https://github.com/vahrervert/cui) and CUI creator **cpores** for the Nighty UI foundation
- [lord-m1nx/nighty-slash-api](https://github.com/lord-m1nx/nighty-slash-api) for the Nighty slash-command framework used as the integration reference
- **nes**, Nighty's owner, for the Nighty Themes V2 layout format used by Components V2 responses
- [Hardik Pandya's Stop Slop](https://github.com/hardikpandya/stop-slop) for the prose checklist used on this README

## License

Nighty VC Manager is released under the [MIT License](https://github.com/aboveproof/Nighty-VC-Manager/blob/main/LICENSE).

