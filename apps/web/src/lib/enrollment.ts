export interface EnrollmentState {
  id: string;
  mesh: { id: string; name: string } | null;
  invite: { id: string; expires: number; ticket: string } | null;
  joining: { name: string; expires: number } | null;
  members: { id: string; name: string; self: boolean; online: boolean }[];
  pending: { id: string; applicant: string; name: string; issuer: string; expires: number }[];
  notice: string;
}
export const isInvitation = (text: string) => text.length <= 4096 && text.trim().startsWith('layercake://join/');
