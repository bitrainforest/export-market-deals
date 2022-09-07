CREATE TABLE IF NOT EXISTS Deals (
    ID TEXT,
    PieceCID TEXT,
    PieceSize INT,
    VerifiedDeal BOOL,
    ClientAddress TEXT,
    ProviderAddress TEXT,
    Label TEXT,
    StartEpoch INT,
    EndEpoch INT,
    StoragePricePerEpoch TEXT,
    ProviderCollateral TEXT,
    ClientCollateral TEXT,
    SectorStartEpoch INT,
    LastUpdatedEpoch INT,
    SlashEpoch INT,
    PRIMARY KEY(ID)
) WITHOUT ROWID;
