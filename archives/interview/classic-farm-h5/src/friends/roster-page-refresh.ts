export const preservePagedRosterBaseline = (
  silent: boolean,
  hadReadyRoster: boolean,
  currentRevision: bigint,
  incomingRevision: bigint,
): boolean => silent && hadReadyRoster && currentRevision !== 0n && currentRevision === incomingRevision;

export const refreshedRosterNextCursor = (
  preserveBaseline: boolean,
  currentNextCursor: string,
  incomingFirstPageCursor: string,
): string => preserveBaseline ? currentNextCursor : incomingFirstPageCursor;
