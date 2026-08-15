import { DurableObject } from 'cloudflare:workers';
import index from './static/index.html';

// source for historic numbers prior to the start of my archiver: myndzi
const PRE_GENESIS_TOTAL = 17750153;
const PRE_GENESIS_UGUU = 160122;
const PRE_GENESIS_PUNCH = 33776;

export class ElasticsearchCounter extends DurableObject<Env> {
  private sessions = new Set<WebSocket>();
  private alarmScheduled = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/websocket') {
      return this.handleWebSocket(request);
    }

    return new Response('Not found', { status: 404 });
  }

  async handleWebSocket(request: Request): Promise<Response> {
    const requestUrl = new URL(request.url);
    if (request.headers.get('Origin') !== requestUrl.origin) {
      return new Response('WebSocket origin not allowed', { status: 403 });
    }

    const upgradeHeader = request.headers.get('Upgrade');
    if (upgradeHeader?.toLowerCase() !== 'websocket') {
      return new Response('Expected websocket', { status: 400 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    this.sessions.add(server);

    if (this.sessions.size === 1) {
      this.startPolling();
    }

    server.addEventListener('close', () => {
      this.sessions.delete(server);
      if (this.sessions.size === 0) {
        this.alarmScheduled = false;
      }
    });

    server.addEventListener('error', () => {
      this.sessions.delete(server);
      if (this.sessions.size === 0) {
        this.alarmScheduled = false;
      }
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private startPolling(): void {
    if (!this.alarmScheduled) {
      this.ctx.storage.setAlarm(Date.now() + 1000);
      this.alarmScheduled = true;
    }
  }

  async alarm(): Promise<void> {
    await this.pollElasticsearch();

    if (this.sessions.size > 0) {
      this.ctx.storage.setAlarm(Date.now() + 1000);
    } else {
      this.alarmScheduled = false;
    }
  }

  private async pollElasticsearch(): Promise<void> {
    try {
      const countQuery = (query: any, agg: boolean = false) => ({
        size: 0,
        query,
        aggs: agg
          ? {
              top_names: {
                terms: {
                  field: 'name',
                  size: 25,
                },
              },
            }
          : undefined,
        track_total_hits: true,
      });

      const privmsg = { term: { 'irc.cmd': 'PRIVMSG' } };
      const totalSearchQuery = countQuery(privmsg);

      const uguuSearchQuery = countQuery(
        {
          bool: {
            must: [privmsg, { prefix: { 'message.keyword': '!uguu' } }],
          },
        },
        true,
      );

      const punchSearchQuery = countQuery(
        {
          bool: {
            must: [privmsg, { prefix: { 'message.keyword': '!punch' } }],
          },
        },
        true,
      );

      const magikarpSearchQuery = countQuery(
        {
          bool: {
            must: [privmsg, { prefix: { 'message.keyword': '!magikarp' } }],
          },
        },
        true,
      );

      // make 1 request instead of 3 to reduce outgoing requests for cf
      const response = await fetch(
        `${this.env.ELASTICSEARCH_URL}/${this.env.ELASTICSEARCH_INDEX}/_msearch`,
        {
          method: 'POST',
          headers: {
            Authorization: `ApiKey ${this.env.ELASTICSEARCH_APIKEY}`,
            'Content-Type': 'application/x-ndjson',
          },
          body:
            '{}\n' +
            JSON.stringify(totalSearchQuery) +
            '\n{}\n' +
            JSON.stringify(uguuSearchQuery) +
            '\n{}\n' +
            JSON.stringify(punchSearchQuery) +
            '\n{}\n' +
            JSON.stringify(magikarpSearchQuery) +
            '\n',
        },
      );

      if (!response.ok) {
        throw new Error(
          `Elasticsearch request failed: ${response.status}\n` +
            (await response.text()),
        );
      }

      const data = (await response.json()) as any;

      const [totalResponse, uguuResponse, punchResponse, magikarpResponse] = data.responses;

      this.broadcast({
        type: 'update',
        totalCount: totalResponse.hits.total.value + PRE_GENESIS_TOTAL,
        uguuCount: uguuResponse.hits.total.value + PRE_GENESIS_UGUU,
        punchCount: punchResponse.hits.total.value + PRE_GENESIS_PUNCH,
        magikarpCount: magikarpResponse.hits.total.value,
        uguuTop: uguuResponse.aggregations.top_names.buckets.map((b: any) => [
          b.key,
          b.doc_count,
        ]),
        punchTop: punchResponse.aggregations.top_names.buckets.map((b: any) => [
          b.key,
          b.doc_count,
        ]),
        magikarpTop: magikarpResponse.aggregations.top_names.buckets.map((b: any) => [
          b.key,
          b.doc_count,
        ]),
        timestamp: Date.now(),
      });
    } catch (error) {
      console.error('Error polling Elasticsearch:', error);
      this.broadcast({
        type: 'error',
        message: 'Failed to fetch count from Elasticsearch',
        timestamp: Date.now(),
      });
    }
  }

  private broadcast(message: any): void {
    const messageStr = JSON.stringify(message);

    for (const session of this.sessions) {
      try {
        session.send(messageStr);
      } catch (error) {
        this.sessions.delete(session);
      }
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/' || url.pathname === '/index.html') {
      return new Response(index, {
        headers: {
          'Cache-Control': 'public, max-age=300',
          'Content-Security-Policy':
            "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self' ws: wss:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
          'Content-Type': 'text/html; charset=utf-8',
          'Permissions-Policy':
            'camera=(), geolocation=(), microphone=(), payment=(), usb=()',
          'Referrer-Policy': 'no-referrer',
          'X-Content-Type-Options': 'nosniff',
        },
      });
    }

    if (url.pathname === '/websocket') {
      const id = env.ELASTICSEARCH_COUNTER.idFromName('counter');
      const durableObject = env.ELASTICSEARCH_COUNTER.get(id);
      return durableObject.fetch(request);
    }

    return new Response('Not found', { status: 404 });
  },
} satisfies ExportedHandler<Env>;
