include env.include

.SUFFIXES:

# #####################################
# Directory paths to search for
# dependencies
#

VPATH = ./$(ENV)/$(SRC)./$(HQL) \
		  ./$(ENV)/$(SRC)/$(META) \
		  ./$(ENV)/$(SRC)/$(FMT) \
		  ./$(ENV)/$(SRC)/$(QC)

all: DIR DATABASE get_table_metadata TGT_METADATA QC_METADATA

.PHONY: DIR
DIR: Metadata Fmt HQL QC

Metadata Fmt HQL QC: 
	[ -d ./$(ENV)/$(SRC)/$@ ] || mkdir -p ./$(ENV)/$(SRC)/$@


# ######################################
# Build sql cmd file if not exists
#
get_table_metadata: get_table_metadata.sql

get_table_metadata.sql:
	echo "SET NOCOUNT ON;" > $@
	echo "SELECT CAST(ORDINAL_POSITION AS VARCHAR) + ',' + " >> $@
	echo "    COLUMN_NAME + ',' + " >> $@
	echo "    DATA_TYPE + ',' +" >> $@
	echo "    CASE DATA_TYPE " >> $@
	echo "      WHEN 'VARCHAR' THEN CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR)" >> $@
	echo "      WHEN 'CHAR' THEN CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR)" >> $@
	echo "      ELSE COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR),'')" >> $@
	echo "    END + ',' +" >> $@
	echo "    COALESCE(CAST(NUMERIC_SCALE AS VARCHAR),'') + ',' +" >> $@
	echo "    CAST(IS_NULLABLE AS VARCHAR)" >> $@
	echo "FROM INFORMATION_SCHEMA.COLUMNS" >> $@
	echo "WHERE TABLE_NAME='$(TableName)' AND" >> $@
	echo "TABLE_SCHEMA = 'dbo';" >> $@
	echo "GO" >> $@

SRC_CONN = -S $(SRC_HOST)\\$(SRC_INST) -d $(SRC_DB) -U $(SRC_USR) -P '$(SRC_PWD)'
SRC_CONN_AON = -S "$(SRC_HOST),$(SRC_PORT)" -d $(SRC_DB) -U $(SRC_USR) -P '$(SRC_PWD)'

TABLES = $(SRC_TABLES)

# ##################################################
# Get the source table counts
#

.PHONY: DATABASE
DATABASE: $(TABLES)

.PHONY: TABLES
$(TABLES): %: %.count %.cols

.ONESHELL:
%.count:
	# Protect the @'s
	[ -s $(SRC)_counts.sql ] && rm $(SRC)_counts.sql
	[ -s ./$(ENV)/$(SRC)/$@ ] && rm ./$(ENV)/$(SRC)/$@
	@echo "SET NOCOUNT ON" > $(SRC)_counts.sql
	@echo "SELECT CONVERT(VARCHAR(20), getdate(), 121) + ',' + '$(ENV)' + ',' + '$(SRC)' + ',' + '$(basename $@)' + ',' + CAST(COUNT(*) AS VARCHAR) FROM $(basename $@);" >> $(SRC)_counts.sql
	@echo "GO" >> $(SRC)_counts.sql

ifeq ($(SRC_AON),N)
	@$(SQLCMD) $(SQLCMD_OPT) \
		$(SRC_CONN) \
		-i $(SRC)_counts.sql >> ./$(ENV)/$(SRC)/Metadata/$@ || echo "**** $(SRC_DB), $@ - $(ENV), count failed"
endif
ifeq ($(SRC_AON),Y)
	@$(SQLCMD) $(SQLCMD_OPT) \
		$(SRC_CONN_AON) \
		-i $(SRC)_counts.sql >> ./$(ENV)/$(SRC)/Metadata/$@ || echo "**** $(SRC_DB), $@ - $(ENV), count failed"
endif

# ##################################################
# Get the source table metadata 
#

# .PHONY: SRC_COLS
# SRC_COLS: $(TABLES)

# $(TABLES): %: %.cols

%.cols:
ifeq ($(SRC_AON),N)
	@$(SQLCMD) $(SQLCMD_OPT) \
		$(SRC_CONN) \
		-v TableName="$(basename $@)" \
		-i get_table_metadata.sql \
		-o ./$(ENV)/$(SRC)/Metadata/$@ || echo "**** $(SRC_DB), $@ - $(ENV), metadata gathering failed"
endif

ifeq ($(SRC_AON),Y)
	@$(SQLCMD) $(SQLCMD_OPT) \
		$(SRC_CONN_AON) \
		-v TableName="$(basename $@)" \
		-i get_table_metadata.sql \
		-o ./$(ENV)/$(SRC)/Metadata/$@ || echo "**** $(SRC_DB), $@ - $(ENV), metadata gathering failed"
endif

	@[ -s ./$(ENV)/$(SRC)/Metadata/$@ ] && echo "**** SUCCESS $(SRC_DB), $@ - $(ENV), metadata gathering successfull"


# ##################################################
# Generate the HQL
#

.PHONY: GENHQL
GENHQL: $(TABLES)

$(TABLES): %: %.hql
%.hql:
	$(GAWK) --assign NCOL=$(DOLLAR)(cat ./$(ENV)/$(SRC)/Metadata/$(basename $@).cols | wc -l) \
		--assign TABLE=$(basename $@) \
		--assign SRC=$(SRC) \
		--assign QDC_SOURCE=$(QDC_SOURCE) \
		--exec=genhql.awk ./$(ENV)/$(SRC)/Metadata/$(basename $@).cols > ./$(ENV)/$(SRC)/HQL/$(SRC)_$@

# ##################################################
# Get the tgt table metadata
#
ifeq "$(TGT_AON)" "Y"
TGT_CONN = -S $(TGT_HOST),$(TGT_PORT) -d $(TGT_DB) -U $(TGT_USR) -P '$(TGT_PWD)'
else
TGT_CONN = -S $(TGT_HOST)\\$(TGT_INST) -d $(TGT_DB) -U $(TGT_USR) -P '$(TGT_PWD)'
endif

PG_CONN = --host=$(PG_HOST) --port=$(PG_PORT) --username=$(PG_USER) --dbname=$(PG_DB)

.PHONY:
TGT_METADATA: $(TGT_TABLES)

$(TGT_TABLES): %: %.fmt 

%.meta:
	# Target table: $(basename $@)
	@$(SQLCMD) $(SQLCMD_OPT) $(TGT_CONN) -v TableName="$(basename $@)" -i get_table_metadata.sql -o ./$(ENV)/$(SRC)/Fmt/$(basename $@).meta || echo "**** $(SRC_DB), $@ - $(ENV), target table metadata gathering failed"

%.fmt:
	$(BCP) $(basename $@) format nul -c \
		$(TGT_CONN) \
		-t '$(BCP_FIELD_TERM)' \
		-r '$(BCP_RECORD_TERM)' \
		-f ./$(ENV)/$(SRC)/Fmt/$(basename $@).fmt

# ##################################################
# Get the QC table metadata
#

.PHONY: QC_METADATA
QC_METADATA: $(TABLES)


$(TABLES): %: %.fld
%.fld:
	PGPASSWORD=$(PG_PASS) $(PSQL) \
		$(PG_CONN) \
		-f ./get_qc_entity.sql \
		-v src="'$(QDC_SOURCE)'" \
		-v ent="'$(basename $@)'" > ./$(ENV)/$(SRC)/QC/$@

