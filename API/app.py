from fastapi import FastAPI, Query
# import GET Request modules

app = FastAPI(title="Cerbyd")

@app.get("/trips")
async def get_trips():
    """
    Return trips data.
    """
    #trips = get_trips()
    #return {"trips": trips}
    return