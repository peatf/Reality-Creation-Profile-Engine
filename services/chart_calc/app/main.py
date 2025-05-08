import json # Added for sorted json dump
import asyncio # Added to resolve NameError
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, Union # pydantic BaseModel, Field are now in request_schemas
from datetime import datetime, timezone # For Kafka timestamp and timezone.utc

# --- Schemas and Services ---
from .schemas.astrology_schemas import AstrologyChartResponse
from .schemas.human_design_schemas import HumanDesignChartResponse
from .schemas.request_schemas import CalculationRequest # Import from new location
from .services.astrology_service import calculate_astrological_chart
from .services.human_design_service import calculate_human_design_chart
from .core.cache import get_from_cache, set_to_cache, startup_redis_event, shutdown_redis_event
from .core.kafka_producer import send_kafka_message, startup_kafka_event, shutdown_kafka_event, CHART_CALCULATED_TOPIC

# --- FastAPI App Instance ---
app = FastAPI(
    title="Chart Calculation Service",
    description="Provides astrology and human design chart calculations. Assumes birth_time is UTC.",
    version="0.1.0",
    on_startup=[startup_redis_event, startup_kafka_event],
    on_shutdown=[shutdown_redis_event, shutdown_kafka_event]
)

# --- Endpoints ---
@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint to verify service availability.
    """
    return {"status": "ok"}

@app.post("/calculate/astrology", response_model=AstrologyChartResponse, tags=["Calculations"])
async def post_calculate_astrology(request: CalculationRequest) -> AstrologyChartResponse:
    """
    Calculates astrological chart data based on birth details.
    \f
    Args:
        request (CalculationRequest): Birth date, time (UTC), latitude, and longitude.
    Returns:
        AstrologyChartResponse: Detailed astrological chart data.
    """
    request_dict = request.model_dump()
    cache_key = f"astrology:{json.dumps(request_dict, sort_keys=True)}" # Ensure consistent key order
    cached_result_str = await get_from_cache(cache_key)
    if cached_result_str:
        try:
            # Assuming cached_result_str is a JSON string of AstrologyChartResponse
            return AstrologyChartResponse.model_validate_json(cached_result_str)
        except Exception as e:
            print(f"Error deserializing cached astrology data: {e}. Recalculating.")
            # Fall through to recalculate if deserialization fails

    try:
        chart_data = await calculate_astrological_chart(request)
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # TODO: Add proper logging
        print(f"Error in astrology calculation: {e}")
        raise HTTPException(status_code=500, detail="Error during astrology calculation.")

    event_payload = {
        "calculation_type": "astrology",
        "request_payload": request.model_dump(),
        "result_summary": { # Keep summary concise
            "num_planets": len(chart_data.planetary_positions),
            "num_houses": len(chart_data.house_cusps),
            "num_aspects": len(chart_data.aspects)
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service_version": app.version
    }
    # Fire-and-forget Kafka message, or await if delivery confirmation is critical and handled by send_kafka_message
    asyncio.create_task(send_kafka_message(CHART_CALCULATED_TOPIC, event_payload, key=request.birth_date))

    # Ensure the value stored in cache is a string. model_dump_json() produces a string.
    await set_to_cache(cache_key, chart_data.model_dump_json(), expire_seconds=24 * 60 * 60) # 24 hours TTL

    return chart_data

@app.post("/calculate/human_design", response_model=HumanDesignChartResponse, tags=["Calculations"])
async def post_calculate_human_design(request: CalculationRequest) -> HumanDesignChartResponse:
    """
    Calculates Human Design chart data based on birth details.
    \f
    Args:
        request (CalculationRequest): Birth date, time (UTC), latitude, and longitude.
    Returns:
        HumanDesignChartResponse: Detailed Human Design chart data.
    """
    request_dict = request.model_dump()
    cache_key = f"humandesign:{json.dumps(request_dict, sort_keys=True)}" # Ensure consistent key order
    cached_result_str = await get_from_cache(cache_key)
    if cached_result_str:
        try:
            # Assuming cached_result_str is a JSON string of HumanDesignChartResponse
            return HumanDesignChartResponse.model_validate_json(cached_result_str)
        except Exception as e:
            print(f"Error deserializing cached human design data: {e}. Recalculating.")
            # Fall through to recalculate if deserialization fails

    try:
        chart_data = await calculate_human_design_chart(request)
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # TODO: Add proper logging
        print(f"Error in Human Design calculation: {e}")
        raise HTTPException(status_code=500, detail="Error during Human Design calculation.")

    event_payload = {
        "calculation_type": "human_design",
        "request_payload": request.model_dump(),
        "result_summary": { # Keep summary concise
            "type": chart_data.type,
            "authority": chart_data.authority,
            "profile": chart_data.profile,
            "definition": chart_data.definition
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service_version": app.version
    }
    # Fire-and-forget Kafka message
    asyncio.create_task(send_kafka_message(CHART_CALCULATED_TOPIC, event_payload, key=request.birth_date))

    # Ensure the value stored in cache is a string. model_dump_json() produces a string.
    await set_to_cache(cache_key, chart_data.model_dump_json(), expire_seconds=24 * 60 * 60) # 24 hours TTL
    return chart_data

# --- Further Development Areas ---
# TODO: Refine error handling and input validation (beyond basic Pydantic and current try-except)
# TODO: Implement comprehensive logging
# TODO: Consider adding a unique request ID for tracing across cache, Kafka, logs.

if __name__ == "__main__":
    import uvicorn
    # This is for local debugging. For production, use the Docker CMD.
    # Ensure services (astrology_service, human_design_service) and schemas are correctly imported.
    # The imports are now relative: from .services.astrology_service import ...
    # To run this directly: python -m app.main (if 'app' is in PYTHONPATH or you are in 'services/chart_calc/')
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)