#!/bin/bash

# 🌌 12306 Next-Gen DevOps Orchestration Command Tool
# Standard Shell compliant, cross-platform friendly.

set -e

# Define color schemes for beautiful outputs
GREEN='\033[0;32m'
BLUE='\033[0;34m'
AMBER='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_banner() {
    echo -e "${BLUE}====================================================${NC}"
    echo -e "${GREEN}    🌌 12306 Next-Gen CQRS Ticketing DevOps Tool     ${NC}"
    echo -e "${BLUE}====================================================${NC}"
}

usage() {
    print_banner
    echo "Usage: ./ops.sh [command]"
    echo ""
    echo "Available commands:"
    echo "  seed     Initialize database schemas and seed default train allocations (G888)"
    echo "  health   Probe and output SRE-grade JSON report from the health endpoint"
    echo "  status   Verify if local Frontend (Port 8080) and Backend (Port 8000) are active"
    echo "  test     Execute full automated test suite (Unit, Integration, BDD, Stress)"
    echo "  locust   Launch Locust distributed load testing dashboard on Port 8089"
    echo ""
}

case "$1" in
    seed)
        print_banner
        echo -e "${AMBER}[Ops] Triggering async database seeder and cache pre-heating...${NC}"
        ./venv/bin/python src/app/ops/seed_db.py
        ;;
    health)
        print_banner
        echo -e "${AMBER}[Ops] Querying the API core health probe...${NC}"
        if curl -s --connect-timeout 2 http://localhost:8000/api/v1/ops/health > /dev/null; then
            echo -e "${GREEN}[OK] Connection succeeded. System Report:${NC}"
            curl -s http://localhost:8000/api/v1/ops/health
            echo ""
        else
            echo -e "${RED}[ERROR] Core API server is unreachable. Please run: uvicorn src.app.main:app --port 8000 first.${NC}"
        fi
        ;;
    status)
        print_banner
        echo -e "${AMBER}[Ops] Checking ports status...${NC}"
        
        # Check Backend
        if curl -s --connect-timeout 1 http://localhost:8000/ > /dev/null; then
            echo -e "${GREEN}[ACTIVE] Backend API Server is active at http://localhost:8000${NC}"
        else
            echo -e "${RED}[INACTIVE] Backend API Server is DOWN at Port 8000${NC}"
        fi

        # Check Frontend
        if curl -s --connect-timeout 1 http://localhost:8080/ > /dev/null; then
            echo -e "${GREEN}[ACTIVE] Frontend Web App is active at http://localhost:8080${NC}"
        else
            echo -e "${RED}[INACTIVE] Frontend Web App is DOWN at Port 8080${NC}"
        fi
        ;;
    test)
        print_banner
        echo -e "${AMBER}[Ops] Executing complete testing pipeline...${NC}"
        ./venv/bin/python -m pytest -v
        ;;
    locust)
        print_banner
        echo -e "${AMBER}[Ops] Launching Locust load testing on Port 8089...${NC}"
        echo -e "${GREEN}[INFO] Please open http://localhost:8089 in your browser to run the stress test.${NC}"
        ./venv/bin/locust -f tests/locustfile.py --host http://localhost:8000
        ;;
    *)
        usage
        exit 1
        ;;
esac
