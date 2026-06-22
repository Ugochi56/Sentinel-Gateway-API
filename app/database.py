import httpx
from postgrest.utils import SyncClient
from supabase import create_client, Client
from app.config import settings

_client: Client | None = None


def get_db() -> Client:
    global _client
    if _client is None:
        _client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        
        # Configure the underlying PostgREST session to use HTTP/1.1 (forces http2=False)
        # and disable TLS session ticket resumption via custom SSLContext.
        # This resolves intermittent SSL decryption and transport bugs on Windows.
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.options |= ssl.OP_NO_TICKET

        session = _client.postgrest.session
        _client.postgrest.session = SyncClient(
            base_url=session.base_url,
            headers=session.headers,
            auth=session.auth,
            http2=False,
            verify=ssl_context,
            timeout=60.0
        )
    return _client
