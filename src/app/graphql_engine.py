import re
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# SDL Schema Definition for 12306 High-Concurrency Ticketing GraphQL API
GRAPHQL_SCHEMA_SDL = """
type Station {
  name: String!
  sequence: Int!
}

type TrainAvailability {
  scheduleId: Int!
  fromStationSeq: Int!
  toStationSeq: Int!
  seatClass: String!
  availableSeats: Int!
}

type Ticket {
  id: String!
  passengerId: String!
  seatNo: String!
  carriageNo: String!
  price: Float!
}

type Order {
  id: String!
  requestId: String!
  reservationId: String!
  state: String!
  totalAmount: Float!
  expiresAt: String!
  tickets: [Ticket!]!
}

type Query {
  # Query ticket availability for a specific route and seat class
  queryAvailability(
    scheduleId: Int!
    fromStationSeq: Int!
    toStationSeq: Int!
    seatClass: String!
  ): TrainAvailability!

  # Retrieve order details by ID
  order(id: String!): Order
}

type Mutation {
  # Trigger active refund for a passenger ticket
  refundOrder(orderId: String!, passengerId: String): Boolean!
}
"""

def extract_fields(query_str: str, start_keyword: str) -> List[Any]:
    """Surgically extracts requested sub-fields inside braces to avoid over-fetching."""
    keyword_idx = query_str.find(start_keyword)
    if keyword_idx == -1:
        return []
    
    open_brace_idx = query_str.find("{", keyword_idx)
    if open_brace_idx == -1:
        return []
    
    # Trace bracket matching to slice the selected GraphQL projection block
    count = 1
    content = ""
    for i in range(open_brace_idx + 1, len(query_str)):
        char = query_str[i]
        if char == "{":
            count += 1
        elif char == "}":
            count -= 1
            if count == 0:
                content = query_str[open_brace_idx + 1:i]
                break
                
    # Tokenize words, nested brackets, and spaces
    tokens = re.split(r'(\s+|{|})', content)
    fields = []
    
    current_nested_name = ""
    i = 0
    while i < len(tokens):
        token = tokens[i].strip()
        if not token:
            i += 1
            continue
            
        if token == "{":
            # Enter nested field selection (e.g., tickets { id seatNo })
            nested_start = i
            count = 1
            for j in range(i + 1, len(tokens)):
                t = tokens[j].strip()
                if t == "{":
                    count += 1
                elif t == "}":
                    count -= 1
                    if count == 0:
                        nested_end = j
                        nested_content_str = "".join(tokens[nested_start + 1:nested_end])
                        # Recurse or list-filter nested elements
                        nested_fields = [
                            f.strip() for f in re.split(r'[\s,]+', nested_content_str) 
                            if f.strip() and f.strip() not in ["{", "}"]
                        ]
                        fields.append((current_nested_name, nested_fields))
                        i = nested_end
                        current_nested_name = ""
                        break
            i += 1
            continue
        elif token == "}":
            i += 1
            continue
        else:
            # Check if this token acts as a parent for a nested block
            is_nested = False
            for k in range(i + 1, len(tokens)):
                next_t = tokens[k].strip()
                if next_t == "{":
                    is_nested = True
                    current_nested_name = token
                    break
                elif next_t:
                    break
            
            if not is_nested:
                fields.append(token)
            i += 1
            
    return fields

async def resolve_graphql_query(db_session, query_str: str) -> Dict[str, Any]:
    """Core GraphQL Resolver engine directing queries and mutations to optimized backend pipelines."""
    # Compress whitespaces for unified regex searching
    query_clean = " ".join(query_str.split())
    
    errors = []
    data = {}
    
    # 1. Resolver: queryAvailability Query
    if "queryAvailability" in query_clean:
        schedule_id_match = re.search(r'scheduleId\s*:\s*(\d+)', query_clean)
        from_seq_match = re.search(r'fromStationSeq\s*:\s*(\d+)', query_clean)
        to_seq_match = re.search(r'toStationSeq\s*:\s*(\d+)', query_clean)
        seat_class_match = re.search(r'seatClass\s*:\s*"([^"]+)"', query_clean)
        
        if schedule_id_match and from_seq_match and to_seq_match and seat_class_match:
            schedule_id = int(schedule_id_match.group(1))
            from_seq = int(from_seq_match.group(1))
            to_seq = int(to_seq_match.group(1))
            seat_class = seat_class_match.group(1)
            
            from src.app.main import query_availability
            try:
                # Call optimized query API (with active Redis timeout fallbacks)
                res = await query_availability(
                    schedule_id=schedule_id,
                    from_station_seq=from_seq,
                    to_station_seq=to_seq,
                    seat_class=seat_class,
                    db=db_session
                )
                
                raw_data = {
                    "scheduleId": schedule_id,
                    "fromStationSeq": from_seq,
                    "toStationSeq": to_seq,
                    "seatClass": seat_class,
                    "availableSeats": res.get("available_seats", 0)
                }
                
                # Perform surgical selection filtering to avoid over-fetching
                fields = extract_fields(query_clean, "queryAvailability")
                filtered_res = {}
                for f in fields:
                    if isinstance(f, str) and f in raw_data:
                        filtered_res[f] = raw_data[f]
                data["queryAvailability"] = filtered_res
            except Exception as e:
                logger.error(f"GraphQL Error: queryAvailability resolution failed: {str(e)}")
                errors.append({"message": f"queryAvailability resolver failed: {str(e)}"})
        else:
            errors.append({"message": "queryAvailability missing required arguments (scheduleId, fromStationSeq, toStationSeq, seatClass)."})
            
    # 2. Resolver: order Detail Query
    if "order" in query_clean and "refundOrder" not in query_clean:
        id_match = re.search(r'order\s*\(\s*id\s*:\s*"([^"]+)"', query_clean)
        if id_match:
            order_id = id_match.group(1)
            
            from src.app.models import Orders, Ticket, Seat
            from sqlalchemy import select
            
            try:
                order_stmt = select(Orders).where(Orders.id == order_id)
                order_obj = (await db_session.execute(order_stmt)).scalar()
                
                if order_obj:
                    # Fetch tickets bound to this order
                    ticket_stmt = select(Ticket).where(Ticket.reservation_id == order_obj.reservation_id)
                    tickets = (await db_session.execute(ticket_stmt)).scalars().all()
                    
                    tickets_data = []
                    for t in tickets:
                        seat_stmt = select(Seat).where(Seat.id == t.seat_id)
                        seat_obj = (await db_session.execute(seat_stmt)).scalar()
                        tickets_data.append({
                            "id": t.id,
                            "passengerId": t.passenger_id,
                            "seatNo": seat_obj.seat_no if seat_obj else "Unknown",
                            "carriageNo": seat_obj.carriage_no if seat_obj else "Unknown",
                            "price": float(t.price)
                        })
                        
                    raw_data = {
                        "id": order_obj.id,
                        "requestId": order_obj.request_id,
                        "reservationId": order_obj.reservation_id,
                        "state": order_obj.state,
                        "totalAmount": float(order_obj.total_amount),
                        "expiresAt": order_obj.expires_at.isoformat() if order_obj.expires_at else "",
                        "tickets": tickets_data
                    }
                    
                    # Apply surgical filtering (including nested lists)
                    fields = extract_fields(query_clean, "order")
                    filtered_res = {}
                    for f in fields:
                        if isinstance(f, str) and f in raw_data:
                            filtered_res[f] = raw_data[f]
                        elif isinstance(f, tuple):
                            key_name, sub_fields = f
                            if key_name == "tickets":
                                filtered_tickets = []
                                for t_dict in tickets_data:
                                    t_filtered = {}
                                    for sf in sub_fields:
                                        if sf in t_dict:
                                            t_filtered[sf] = t_dict[sf]
                                    filtered_tickets.append(t_filtered)
                                filtered_res[key_name] = filtered_tickets
                    data["order"] = filtered_res
                else:
                    data["order"] = None
            except Exception as e:
                logger.error(f"GraphQL Error: order query failed for ID {order_id}: {str(e)}")
                errors.append({"message": f"order resolver failed: {str(e)}"})
        else:
            errors.append({"message": "order query missing required parameter (id)."})

    # 3. Resolver: refundOrder Mutation
    if "refundOrder" in query_clean:
        order_id_match = re.search(r'orderId\s*:\s*"([^"]+)"', query_clean)
        passenger_id_match = re.search(r'passengerId\s*:\s*"([^"]+)"', query_clean)
        
        if order_id_match:
            order_id = order_id_match.group(1)
            passenger_id = passenger_id_match.group(1) if passenger_id_match else None
            
            from src.app.order_service import OrderService
            try:
                await OrderService.refund_order(db_session=db_session, order_id=order_id, passenger_id=passenger_id)
                await db_session.commit()
                data["refundOrder"] = True
            except Exception as e:
                await db_session.rollback()
                logger.error(f"GraphQL Error: refundOrder execution failed: {str(e)}")
                errors.append({"message": f"refundOrder execution failed: {str(e)}"})
                data["refundOrder"] = False
        else:
            errors.append({"message": "refundOrder missing required parameter (orderId)."})
            
    response = {}
    if data:
        response["data"] = data
    if errors:
        response["errors"] = errors
    return response
