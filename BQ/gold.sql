drop table `clear-basis-469515-u9.gold_dataset.provider_charge_summary`;
create table if not exists `clear-basis-469515-u9.gold_dataset.provider_charge_summary` (
    Provider_Name STRING,
    Dept_Name STRING,
    Amount  FLOAT64
);

TRUNCATE TABLE `clear-basis-469515-u9.gold_dataset.provider_charge_summary`;

INSERT INTO `clear-basis-469515-u9.gold_dataset.provider_charge_summary`
SELECT
     CONCAT(p.firstname, ' ', p.LastName) as Provider_Name,
     d.Name AS Dept_name,
     SUM(t.amount) as amount
from `clear-basis-469515-u9.silver_dataset.transactions` t 
LEFT JOIN `clear-basis-469515-u9.silver_dataset.providers` p 
     on SPLIT(p.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `clear-basis-469515-u9.silver_dataset.departments` d 
     on SPLIT(d.Dept_id, "-")[SAFE_OFFSET(0)] = p.DeptID
where t.is_quarantined = FALSE and d.Name is not null
group by Provider_Name, Dept_name;

CREATE TABLE IF NOT EXISTS `clear-basis-469515-u9.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
);

# TRUNCATE TABLE
TRUNCATE TABLE `clear-basis-469515-u9.gold_dataset.provider_performance`;

# INSERT DATA
INSERT INTO `clear-basis-469515-u9.gold_dataset.provider_performance`
SELECT 
    pr.ProviderID,
    pr.FirstName,
    pr.LastName,
    pr.Specialization,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate
FROM `clear-basis-469515-u9.silver_dataset.providers` pr
LEFT JOIN `clear-basis-469515-u9.silver_dataset.encounters` e 
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `clear-basis-469515-u9.silver_dataset.transactions` t 
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `clear-basis-469515-u9.silver_dataset.claims` c 
    ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;
CREATE TABLE IF NOT EXISTS `clear-basis-469515-u9.gold_dataset.department_performance` (
    Dept_Id STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
);

# TRUNCATE TABLE
TRUNCATE TABLE `clear-basis-469515-u9.gold_dataset.department_performance`;

# INSERT DATA
INSERT INTO `clear-basis-469515-u9.gold_dataset.department_performance`
SELECT 
    d.Dept_Id,
    d.Name AS DepartmentName,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `clear-basis-469515-u9.silver_dataset.departments` d
LEFT JOIN `clear-basis-469515-u9.silver_dataset.encounters` e 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `clear-basis-469515-u9.silver_dataset.transactions` t 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;


