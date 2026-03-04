from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List

async def calculate_monthly_revenue(property_id: str, tenant_id: str, month: int, year: int, db_session=None) -> Decimal:
    """
    Calculates revenue for a specific month, classifying each reservation by its
    check-in date converted to the property's local timezone.
    """
    start_date = datetime(year, month, 1)
    end_date = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)

    try:
        from app.core.database_pool import db_pool
        from sqlalchemy import text

        if not db_pool.session_factory:
            raise Exception("Database pool not available")

        async with db_pool.get_session() as session:
            query = text("""
                SELECT SUM(r.total_amount) AS total
                FROM reservations r
                JOIN properties p
                  ON p.id = r.property_id AND p.tenant_id = r.tenant_id
                WHERE r.property_id = :property_id
                  AND r.tenant_id   = :tenant_id
                  AND (r.check_in_date AT TIME ZONE p.timezone) >= :start_date
                  AND (r.check_in_date AT TIME ZONE p.timezone) <  :end_date
            """)
            result = await session.execute(query, {
                "property_id": property_id,
                "tenant_id":   tenant_id,
                "start_date":  start_date,
                "end_date":    end_date,
            })
            row = result.fetchone()
            return Decimal(str(row.total)) if row and row.total is not None else Decimal("0")

    except Exception as e:
        print(f"Database error in calculate_monthly_revenue for {property_id} (tenant: {tenant_id}): {e}")
        return Decimal("0")

async def calculate_total_revenue(property_id: str, tenant_id: str) -> Dict[str, Any]:
    """
    Aggregates revenue from database.
    """
    try:
        from app.core.database_pool import db_pool


        if db_pool.session_factory:
            async with db_pool.get_session() as session:
                # Use SQLAlchemy text for raw SQL
                from sqlalchemy import text
                
                query = text("""
                    SELECT 
                        property_id,
                        SUM(total_amount) as total_revenue,
                        COUNT(*) as reservation_count
                    FROM reservations 
                    WHERE property_id = :property_id AND tenant_id = :tenant_id
                    GROUP BY property_id
                """)
                
                result = await session.execute(query, {
                    "property_id": property_id, 
                    "tenant_id": tenant_id
                })
                row = result.fetchone()
                
                if row:
                    total_revenue = Decimal(str(row.total_revenue))
                    return {
                        "property_id": property_id,
                        "tenant_id": tenant_id,
                        "total": str(total_revenue),
                        "currency": "USD", 
                        "count": row.reservation_count
                    }
                else:
                    # No reservations found for this property
                    return {
                        "property_id": property_id,
                        "tenant_id": tenant_id,
                        "total": "0.00",
                        "currency": "USD",
                        "count": 0
                    }
        else:
            raise Exception("Database pool not available")
            
    except Exception as e:
        print(f"Database error for {property_id} (tenant: {tenant_id}): {e}")
        
        return {
            "property_id": property_id,
            "tenant_id": tenant_id,
            "total": "0.00",
            "currency": "USD",
            "count": 0
        }
