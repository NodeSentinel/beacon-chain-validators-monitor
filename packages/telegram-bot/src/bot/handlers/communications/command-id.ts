/**
 * Parses the communication id argument of an admin command.
 * Commands that take an id all share the "/command <id>" shape, so the first
 * argument is read positionally and rejected unless it is a positive integer.
 */
export function parseCommunicationIdArgument(text: string | undefined): number | null {
  if (!text) return null;

  const [, rawId] = text.trim().split(/\s+/, 2);
  if (!rawId) return null;

  const communicationId = Number.parseInt(rawId, 10);

  return Number.isSafeInteger(communicationId) && communicationId > 0 ? communicationId : null;
}
