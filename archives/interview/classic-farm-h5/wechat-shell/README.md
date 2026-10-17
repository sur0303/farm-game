# Classic Farm WeChat shell

This directory is the minimal WeChat Mini Program shell for the H5 client.
It deliberately contains platform code only:

```text
wx.login()
  -> POST /api/v1/auth/wx-miniprogram-login
  -> short-lived launch_ticket
  -> web-view opens the H5 with launch_ticket
```

The shell must not contain `AppSecret`, `session_key`, wallet state, crop
state, or theft settlement logic. Those belong to the server or the H5 client
boundary described in `../h5/SERVER-INTEGRATION.md`.

## Before using the shell

1. Copy `config.example.js` to `config.js`.
2. Set `apiBaseUrl` to the HTTPS domain accepted by the Mini Program.
3. Set `h5BaseUrl` to the HTTPS domain configured for `web-view`.
4. Configure the Mini Program request and business domains in the WeChat
   console.
5. Make the server implement the response documented below.

The shell preserves `share_token` through the native share landing page:

好友打开 /pages/share/share?share_token=...
  -> share page keeps the token
  -> “进入农场” redirects to /pages/game/game?share_token=...
  -> game page appends it to the H5 URL alongside launch_ticket
  -> H5 login resolves and accepts it through the server boundary

The shell does not grant access to another player's farm; the server must still
validate and accept the token.

## Server contract

```http
POST {apiBaseUrl}/api/v1/auth/wx-miniprogram-login
Content-Type: application/json
```

```json
{
  "code": "one-time-wx-login-code"
}
```

The response must be JSON with a short-lived, one-time ticket:

```json
{
  "launch_ticket": "short-lived-launch-ticket"
}
```

The shell never receives or stores `AppSecret` or `session_key`.
