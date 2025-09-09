// /api/branch-to-amplitude.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import crypto from 'crypto';

type BranchPayload = {
  name?: 'INSTALL' | 'REINSTALL' | 'OPEN' | string;         // event type
  event?: string;                                           // sometimes 'INSTALL' appears here
  timestamp?: number | string;
  timestamp_millis?: number;
  user_data?: {
    idfa?: string | null;
    idfv?: string | null;
    adid?: string | null;           // GAID/ADID on Android
    os?: string | null;
    os_version?: string | null;
    app_version?: string | null;
    device_model?: string | null;
  };
  last_attributed_touch_data?: {
    channel?: string | null;
    campaign?: string | null;
    ad_partner?: string | null;
    ad_set?: string | null;
    ad_set_id?: string | null;
    campaign_id?: string | null;
    creative?: string | null;
    feature?: string | null;        // ads, email, referral, etc.
    tags?: string[] | null;
    link_id?: string | null;
    web_to_app?: boolean | null;
  };
  // fallbacks that sometimes appear in other payload shapes
  data?: unknown;
  id?: string;                      // event id if provided
};

const AMPLITUDE_KEY = process.env.AMPLITUDE_API_KEY!;
const AMPLITUDE_ENDPOINT = (process.env.AMPLITUDE_HTTP_ENDPOINT || "https://api2.amplitude.com").replace(/\/+$/, "");
const BRANCH_TOKEN = process.env.BRANCH_TOKEN!;

// pick timestamp from a few candidate fields and coerce to millis
function toMillis(ts: number | string | Date | null | undefined): number {
  if (ts === undefined || ts === null) return Date.now();
  if (ts instanceof Date) {
    const t = ts.getTime();
    return t || Date.now();
  }
  if (typeof ts === "number") return ts > 1e12 ? ts : ts * 1000;
  const n = Number(ts);
  if (!Number.isNaN(n)) return n > 1e12 ? n : n * 1000;
  const d = new Date(ts);
  return d.getTime() || Date.now();
}

function pickDeviceId(p: BranchPayload): string {
  const u = (p.user_data || {}) as Record<string, unknown>;

  // Normalize keys to lowercase so we catch both `idfa` and `IDFA`
  for (const key of Object.keys(u)) {
    const lower = key.toLowerCase();
    if (["idfa", "idfv", "adid", "advertising_id", "gaid"].includes(lower)) {
      const value = u[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
  }

  // fallback if nothing found
  return "fallback-" + Date.now();
}



function normalizeEventType(p: BranchPayload): { type: "install" | "reinstall" | "open" | "other"; label: string } {
  const raw = (p.name || p.event || "").toUpperCase();
  if (raw.includes("REINSTALL")) return { type: "reinstall", label: "Branch Reinstall" };
  if (raw.includes("INSTALL"))   return { type: "install",   label: "Branch Attributed Install" };
  if (raw.includes("OPEN"))      return { type: "open",      label: "Branch Open" };
  return { type: "other", label: raw || "Branch Event" };
}

function buildInsertId(p: BranchPayload, when: number, label: string): string {
  // stable hash to dedupe retries
  const seed = JSON.stringify({ id: p.id, name: p.name, event: p.event, when, label, link: p.last_attributed_touch_data?.link_id });
  return crypto.createHash("sha256").update(seed).digest("hex").slice(0, 64);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    if (req.method !== "POST") return res.status(405).send("Method Not Allowed");
    // Simple auth via token in the webhook URL: https://your.app/api/branch-to-amplitude?token=XYZ
    if (!BRANCH_TOKEN || req.query.token !== BRANCH_TOKEN) return res.status(401).send("Unauthorized");

    const payload = (req.body || {}) as BranchPayload;

    // Some Branch setups wrap the event under `data`
    const p: BranchPayload = typeof payload?.data === "object" ? { ...payload, ...payload.data } : payload;

    const deviceId = pickDeviceId(p);
    if (!deviceId) {
      // No stable device id; skip to avoid polluting Amplitude with un-mergeable users
      return res.status(202).json({ skipped: true, reason: "no_device_id" });
    }

    const latd = p.last_attributed_touch_data || {};
    const when = toMillis(p.timestamp_millis || p.timestamp);
    const { type, label } = normalizeEventType(p);
    const insertId = buildInsertId(p, when, label);

    // Build Identify: first-touch ($setOnce) and last-touch ($set) user properties
    const identification = {
      device_id: deviceId,
      user_id: null,
      user_properties: {
        $setOnce: {
          acq_channel_first: latd.channel ?? null,
          acq_campaign_first: latd.campaign ?? null,
          acq_partner_first: latd.ad_partner ?? null,
          acq_adset_first: latd.ad_set ?? null,
          acq_creative_first: latd.creative ?? null,
          acq_install_ts: type === "install" ? when : undefined,
        },
        $set: {
          acq_channel_last: latd.channel ?? null,
          acq_campaign_last: latd.campaign ?? null,
          acq_partner_last: latd.ad_partner ?? null,
          acq_adset_last: latd.ad_set ?? null,
          acq_creative_last: latd.creative ?? null,
          acq_last_touch_ts: when,
        },
      },
    } as const;

    // Build $identify event
    const identifyEvent = {
      device_id: deviceId,
      user_id: null,
      event_type: "$identify",
      user_properties: identification.user_properties,
      time: when,
    };

    // Build attribution event (for QA/funnels)
    const u: Partial<NonNullable<BranchPayload["user_data"]>> = p.user_data ?? {};
    const branchEvent = {
      device_id: deviceId,
      user_id: null,
      event_type: label,
      time: when,
      insert_id: insertId,
      event_properties: {
        install_type: type,
        branch_channel: latd.channel ?? null,
        branch_campaign: latd.campaign ?? null,
        branch_partner: latd.ad_partner ?? null,
        branch_adset: latd.ad_set ?? null,
        branch_creative: latd.creative ?? null,
        branch_feature: latd.feature ?? null,
        branch_link_id: latd.link_id ?? null,
        web_to_app: !!latd.web_to_app,
      },
      app_version: ("app_version" in u && typeof u.app_version === "string") ? u.app_version : undefined,
      platform: ("os" in u && typeof u.os === "string" ? u.os : "").toLowerCase().includes("android") ? "Android" : "iOS",
      os_name: ("os" in u && typeof u.os === "string") ? u.os : undefined,
      os_version: ("os_version" in u && typeof u.os_version === "string") ? u.os_version : undefined,
      device_model: ("device_model" in u && typeof u.device_model === "string") ? u.device_model : undefined,
    };

    const combinedPayload = {
      api_key: AMPLITUDE_KEY,
      events: [identifyEvent, branchEvent],
    };

    console.log("Outgoing Event payload:", JSON.stringify(combinedPayload, null, 2));

    const evtRes = await fetch(`${AMPLITUDE_ENDPOINT}/2/httpapi`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(combinedPayload),
    });

    if (!evtRes.ok) {
      const text = await evtRes.text();
      return res.status(500).json({ error: "event_failed", status: evtRes.status, body: text });
    }

    return res.status(200).json({ ok: true });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: "server_error", message });
  }
}


