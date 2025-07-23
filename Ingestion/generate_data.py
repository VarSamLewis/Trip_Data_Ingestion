from faker import Faker
from datetime import datetime, timedelta
import uuid, random
import json

fake = Faker()

def generate_event(trip_id, rider_id, driver_id, checkpoint):
    location = {
        "lat": float(round(fake.latitude(), 6)),
        "lon": float(round(fake.longitude(), 6)),
        "address": fake.address().replace("\n", ", ")
    }

    status = random.choices(
        ["completed", "cancelled", "no_show"],
        weights=[0.85, 0.10, 0.05],
        k=1
    )[0]

    return {
        "trip_id": trip_id,
        "rider_id": rider_id,
        "driver_id": driver_id,
        "timestamp": datetime.utcnow().isoformat(),
        "location": location,
        "status": status,
        "checkpoint": checkpoint
    }

# Generate sample batchen
def generate_batch(num_trips=10):
    drivers = [f"driver-{i}" for i in range(25)]
    events = []

    for _ in range(num_trips):
        trip_id = str(uuid.uuid4())
        rider_id = f"rider-{random.randint(1000, 9999)}"
        driver_id = random.choice(drivers)

        # Generate pickup and dropoff (or cancelled)
        events.append(generate_event(trip_id, rider_id, driver_id, "pickup"))
        events.append(generate_event(trip_id, rider_id, driver_id, random.choice(["dropoff", "cancelled"])))

    return events

if __name__ == "__main__":
    batch = generate_batch()
    for event in batch:
        print(json.dumps(event, indent = 4))
