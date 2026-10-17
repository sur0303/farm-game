const MAX_SHARE_TOKEN_LENGTH = 512;

export function readShareToken(href = window.location.href): string | null {
  const token = new URL(href).searchParams.get('share_token')?.trim() ?? '';
  if (!token || token.length > MAX_SHARE_TOKEN_LENGTH) return null;
  return token;
}

export function consumeShareTokenFromUrl(href = window.location.href): void {
  if (typeof window === 'undefined' || window.location.href !== href || !window.history?.replaceState) return;
  const url = new URL(href);
  url.searchParams.delete('share_token');
  window.history.replaceState(window.history.state, document.title, url.toString());
}
