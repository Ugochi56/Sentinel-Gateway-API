from pydantic import BaseModel

class TierLimits(BaseModel):
    max_endpoints: int | None  # None means unlimited
    max_payload_bytes: int
    max_retries: int
    retention_days: int

# Tier definitions matching user specifications:
# Free: 5 endpoints, 1MB payload, 5 retries, 3 days retention
# Basic: 25 endpoints, 5MB payload, 10 retries, 7 days retention
# Pro: 100 endpoints, 10MB payload, 20 retries, 15 days retention
# Enterprise: Unlimited endpoints, 25MB payload, 30 retries, 30 days retention
TIERS: dict[str, TierLimits] = {
    "Free": TierLimits(
        max_endpoints=5,
        max_payload_bytes=1 * 1024 * 1024,
        max_retries=5,
        retention_days=3,
    ),
    "Basic": TierLimits(
        max_endpoints=25,
        max_payload_bytes=5 * 1024 * 1024,
        max_retries=10,
        retention_days=7,
    ),
    "Pro": TierLimits(
        max_endpoints=100,
        max_payload_bytes=10 * 1024 * 1024,
        max_retries=20,
        retention_days=15,
    ),
    "Enterprise": TierLimits(
        max_endpoints=None,  # Unlimited
        max_payload_bytes=25 * 1024 * 1024,
        max_retries=30,
        retention_days=30,
    ),
}

def get_tier_limits(plan: str | None) -> TierLimits:
    """
    Returns the TierLimits object for the given plan.
    Defaults to the 'Free' tier if the plan name is not recognized or is None.
    """
    if not plan:
        return TIERS["Free"]
    
    # Normalize naming (e.g. "free" or "FREE" -> "Free")
    norm = plan.strip().capitalize()
    if norm in TIERS:
        return TIERS[norm]
        
    return TIERS["Free"]
