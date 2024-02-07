-- ALTER MATERIALIZED VIEW TO GET CLIENTS TABLE
DROP MATERIALIZED VIEW IF EXISTS api_cnpj_collect;    
CREATE MATERIALIZED VIEW api_cnpj_collect AS
SELECT 
    cli.cnpj AS tx_id 
FROM 
    clients AS cli -- ALTER TO YOUR TABLE CLIENT
WHERE 
    NOT EXISTS (SELECT 1 FROM consolidated_api_clients AS cns WHERE cns."taxId" = cli.cnpj)
;

-- API DATA RETURN (RAW)
DROP TABLE consolidated_api_clients;    
CREATE TABLE consolidated_api_clients
(
    "taxId" VARCHAR(255),
    "company" TEXT,
    "alias" VARCHAR(255),
    "founded" VARCHAR(255),
    "mainActivity" TEXT,
    "sideActivities" TEXT,
    "head" BOOL,
    "status" VARCHAR(255),
    "statusDate" DATE,
    "address" VARCHAR(255),
    "phones" VARCHAR(255),
    "emails" VARCHAR(255),
    "updated" TIMESTAMP
);    
