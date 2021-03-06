use dms;
DELIMITER //
CREATE PROCEDURE exportCsv(IN tblName TEXT)
BEGIN
    DECLARE FieldList,DataTypeListStr,FieldListStr,FOLDER,PREFIX,EXT TEXT;
	SET FieldList = (SELECT GROUP_CONCAT(CONCAT("IFNULL(",COLUMN_NAME,",'') AS ", COLUMN_NAME)) as GroupName
	from INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = 'tblchucvu'
	order BY ORDINAL_POSITION);

	SET DataTypeListStr = (SELECT GROUP_CONCAT(CONCAT("'",DATA_TYPE,"'")) as DataType
	from INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = 'tblchucvu'
	order BY ORDINAL_POSITION);

	SET FieldListStr = (SELECT GROUP_CONCAT(CONCAT("'",COLUMN_NAME,"'")) as GroupName
	from INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = 'tblchucvu'
	order BY ORDINAL_POSITION);
    
	SET FOLDER = REPLACE(@@secure_file_priv,"","");
    
	SET PREFIX = 'tblchucvu';
	SET EXT    = '.csv';
	 
	SET @CMD = CONCAT("
	SELECT ",FieldListStr,"
	UNION ALL
	SELECT ",DataTypeListStr,
	" UNION ALL
	SELECT ",FieldList," FROM tblchucvu INTO OUTFILE '",FOLDER,PREFIX,EXT,
					   "' FIELDS ENCLOSED BY ','", 
                       " TERMINATED BY ';'",
                       " ESCAPED BY '' ",
					   " LINES TERMINATED BY '' ");
	PREPARE statement FROM @CMD;
	EXECUTE statement;
END; //
DELIMITER ;
CALL exportCsv("tblchucvu");