export interface SharePreview {
  inviterLabel: string;
}

export interface ShareResolution {
  targetOwnerId: string;
  inviterLabel: string;
}

export interface ShareIntentResolver {
  resolve(token: string): Promise<SharePreview>;
  accept(token: string, viewerId: string): Promise<ShareResolution>;
}

export interface ShareLinkCreator {
  create(viewerId: string, origin: string): Promise<{ url: string }>;
}
