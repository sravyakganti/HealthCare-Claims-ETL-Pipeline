-- Query 1: Identify High-Cost Diagnosis Codes
-- Used to track cost drivers for the insurance plan
SELECT 
    diagnosis_code,
    COUNT(claim_id) as claim_volume,
    SUM(claim_amount) as total_cost,
    AVG(claim_amount) as avg_cost_per_claim
FROM claims_fact_table
WHERE status = 'PAID'
GROUP BY diagnosis_code
ORDER BY total_cost DESC;

-- Query 2: Calculate Denial Rates by Month (Window Function)
-- Used to monitor provider compliance
WITH MonthlyStats AS (
    SELECT 
        strftime('%Y-%m', date_of_service) as service_month,
        COUNT(*) as total_claims,
        SUM(CASE WHEN status = 'DENIED' THEN 1 ELSE 0 END) as denied_claims
    FROM claims_fact_table
    GROUP BY 1
)
SELECT 
    service_month,
    total_claims,
    denied_claims,
    ROUND((denied_claims * 1.0 / total_claims) * 100, 2) as denial_rate_percentage
FROM MonthlyStats
ORDER BY service_month DESC;