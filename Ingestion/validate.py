class TripEvent(BaseModel):
    trip_id: str
    rider_id: str
    driver_id: str
    timestamp: str
    Location: dict  # Or a nested model if preferred
    status: str
    checkpoint: str
