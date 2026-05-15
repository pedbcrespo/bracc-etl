#!/usr/bin/env bash
# orchestrator.sh — Setup + Download + Importação sequencial
# Uso: bash orchestrator.sh [fonte1 fonte2 ...]
#      bash orchestrator.sh --force fonte1  # força reimportação
# Logs: pipeline_imports.log
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETL_DIR="$ROOT/etl"
DATA_DIR="$ROOT/data"
LOG="$ROOT/pipeline_imports.log"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-changeme}"
INSTALLED_FLAG="$ROOT/.installed"

# ── MAPA fonte → label Neo4j ──────────────────────────────────────────────────
declare -A LABEL_MAP
LABEL_MAP[bcb]="Sanction"
LABEL_MAP[bndes]="Payment"
LABEL_MAP[camara]="Expense"
LABEL_MAP[camara_inquiries]="Inquiry"
LABEL_MAP[ceaf]="Expulsion"
LABEL_MAP[cepim]="BarredNGO"
LABEL_MAP[cnpj]="Company"
LABEL_MAP[comprasnet]="Contract"
LABEL_MAP[cpgf]="GovCardExpense"
LABEL_MAP[cvm]="CVMProceeding"
LABEL_MAP[cvm_funds]="Fund"
LABEL_MAP[datasus]="Health"
LABEL_MAP[eu_sanctions]="InternationalSanction"
LABEL_MAP[holdings]="Partner"
LABEL_MAP[ibama]="Sanction"
LABEL_MAP[icij]="OffshoreEntity"
LABEL_MAP[inep]="MunicipalGazetteAct"
LABEL_MAP[leniency]="LeniencyAgreement"
LABEL_MAP[ofac]="InternationalSanction"
LABEL_MAP[opensanctions]="GlobalPEP"
LABEL_MAP[pgfn]="Sanction"
LABEL_MAP[querido_diario]="MunicipalGazetteAct"
LABEL_MAP[renuncias]="TaxWaiver"
LABEL_MAP[sanctions]="Sanction"
LABEL_MAP[senado]="Expense"
LABEL_MAP[senado_cpis]="CPI"
LABEL_MAP[siconfi]="MunicipalFinance"
LABEL_MAP[siop]="Amendment"
LABEL_MAP[tce_am]="Contract"
LABEL_MAP[tcu]="Sanction"
LABEL_MAP[tesouro_emendas]="Amendment"
LABEL_MAP[transferegov]="Payment"
LABEL_MAP[transparencia]="Contract"
LABEL_MAP[tse]="Election"
LABEL_MAP[tse_bens]="Person"
LABEL_MAP[tse_filiados]="Person"
LABEL_MAP[un_sanctions]="InternationalSanction"
LABEL_MAP[viagens]="GovTravel"
LABEL_MAP[world_bank]="InternationalSanction"

# ── FONTES A IGNORAR ─────────────────────────────────────────────────────────
SKIP=(
    pncp            # roda em background separado
    senado          # dando problema — aguardar fix
    camara          # importação travando — aguardar fix do colega
)

# ── FILA PADRÃO ───────────────────────────────────────────────────────────────
DEFAULT_QUEUE=(
    tesouro_emendas
    bcb
    ceaf
    cepim
    un_sanctions
    leniency
    cvm
    cvm_funds
    ofac
    holdings
    senado_cpis
    sanctions
    querido_diario
    camara_inquiries
    cpgf
    renuncias
    datasus
    icij
    siconfi
    siop
    opensanctions
    transparencia
    tse
    viagens
    cnpj
)

# ─────────────────────────────────────────────────────────────────────────────
CURRENT_FONTE=""
CURRENT_PID=""
FORCE_REIMPORT=0

graceful_shutdown() {
    echo "" | tee -a "$LOG"
    echo "  ┌─────────────────────────────────────────────┐" | tee -a "$LOG"
    echo "  │  ⚠️   INTERRUPÇÃO DETECTADA (Ctrl+C)         │" | tee -a "$LOG"
    echo "  │  Encerrando com segurança...                 │" | tee -a "$LOG"
    echo "  └─────────────────────────────────────────────┘" | tee -a "$LOG"
    if [[ -n "$CURRENT_PID" ]] && kill -0 "$CURRENT_PID" 2>/dev/null; then
        echo "  │  Aguardando processo atual terminar..." | tee -a "$LOG"
        kill -TERM "$CURRENT_PID" 2>/dev/null
        local waited=0
        while kill -0 "$CURRENT_PID" 2>/dev/null && [[ $waited -lt 30 ]]; do
            sleep 1; waited=$((waited + 1)); echo -n "." >&2
        done
        echo "" | tee -a "$LOG"
        kill -0 "$CURRENT_PID" 2>/dev/null && kill -9 "$CURRENT_PID" 2>/dev/null
        echo "  │  ✅ Processo encerrado" | tee -a "$LOG"
    fi
    echo "" | tee -a "$LOG"
    if [[ -n "$CURRENT_FONTE" ]]; then
        echo "  │  Parou em: $CURRENT_FONTE" | tee -a "$LOG"
        echo "  │  Para retomar: bash orchestrator.sh $CURRENT_FONTE" | tee -a "$LOG"
        echo "  │  Para forçar reimportação: bash orchestrator.sh --force $CURRENT_FONTE" | tee -a "$LOG"
    fi
    echo "  ⛔  ENCERRADO — $(date '+%d/%m/%Y %H:%M')" | tee -a "$LOG"
    echo "  💡  PNCP continua em background" | tee -a "$LOG"
    echo "" | tee -a "$LOG"
    exit 0
}
trap graceful_shutdown INT TERM

# ─────────────────────────────────────────────────────────────────────────────
log_blank() { echo "" | tee -a "$LOG"; }
log_banner() {
    echo "" | tee -a "$LOG"
    echo "  ══════════════════════════════════════════════" | tee -a "$LOG"
    echo "  $1" | tee -a "$LOG"
    echo "  ══════════════════════════════════════════════" | tee -a "$LOG"
    echo "" | tee -a "$LOG"
}
log_section() { echo "" | tee -a "$LOG"; echo "  ┌─ $*" | tee -a "$LOG"; echo "" | tee -a "$LOG"; }
log_ok()   { echo "  └─ ✅  $*" | tee -a "$LOG"; echo "" | tee -a "$LOG"; }
log_err()  { echo "  └─ ❌  $*" | tee -a "$LOG"; echo "" | tee -a "$LOG"; }
log_skip() { echo "  └─ ⏭   $*" | tee -a "$LOG"; echo "" | tee -a "$LOG"; }
log_info() { echo "  │  $*" | tee -a "$LOG"; }
beep()       { powershell.exe -Command "[console]::beep(1000,300); [console]::beep(1200,300)" 2>/dev/null || true; }
beep_error() { powershell.exe -Command "[console]::beep(400,500); [console]::beep(400,500)" 2>/dev/null || true; }

filter_output() {
    grep -v "^  File \|^    \|^Traceback (most recent\|site-packages\|__main__\|sys\.exit\|\.invoke\|\.main\|\.callback\|frozen\|_run_module\|_run_code\|_process_result"
}

is_skipped() {
    local fonte="$1"
    for s in "${SKIP[@]}"; do [[ "$s" == "$fonte" ]] && return 0; done
    return 1
}

is_imported() {
    local fonte="$1"
    grep -q "^imported: $fonte " "$INSTALLED_FLAG" 2>/dev/null
}

mark_imported() {
    local fonte="$1"
    local tmpfile
    tmpfile=$(mktemp)
    grep -v "^imported: $fonte " "$INSTALLED_FLAG" > "$tmpfile" 2>/dev/null && mv "$tmpfile" "$INSTALLED_FLAG" || rm -f "$tmpfile"
    echo "imported: $fonte $(date '+%Y-%m-%d %H:%M:%S')" >> "$INSTALLED_FLAG"
}

neo4j_count() {
    local label="$1"
    docker exec bracc-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
        "MATCH (n:$label) RETURN count(n) as total" 2>/dev/null \
        | grep -E '^[0-9]+$' | head -1
}

# ── SYNC NEO4J → .installed ───────────────────────────────────────────────────
# Lê o banco antes de qualquer fila. Registra no .installed fontes que já têm
# dados no Neo4j mas não estão registradas — evita reimportação desnecessária.
sync_neo4j_to_installed() {
    log_banner "🗄️   LENDO NEO4J"
    local synced=0
    local already=0
    local zero=0

    for fonte in "${!LABEL_MAP[@]}"; do
        local label="${LABEL_MAP[$fonte]}"
        local count
        count=$(neo4j_count "$label")
        if [[ -n "$count" ]] && [[ "$count" -gt 0 ]]; then
            if is_imported "$fonte"; then
                already=$((already + 1))
            else
                mark_imported "$fonte"
                synced=$((synced + 1))
            fi
        else
            zero=$((zero + 1))
        fi
    done

    log_info "Sync: $((already + synced)) fontes com dados no banco | $synced novas registradas | $zero sem dados"
    log_blank
}

# ── SETUP ────────────────────────────────────────────────────────────────────
run_setup() {
    log_banner "🔧  SETUP INICIAL"
    local errors=0
    for tool in docker git; do
        if ! command -v $tool &>/dev/null; then
            log_err "$tool não encontrado"
            errors=$((errors + 1))
        else
            log_ok "$tool OK"
        fi
    done
    if ! command -v uv &>/dev/null && ! ~/.local/bin/uv --version &>/dev/null 2>&1; then
        log_err "uv não encontrado. Instale: curl -LsSf https://astral.sh/uv/install.sh | sh"
        errors=$((errors + 1))
    else
        log_ok "uv OK"
    fi
    [[ $errors -gt 0 ]] && { log_err "$errors dependência(s) faltando"; exit 1; }
    log_info "Instalando dependências Python..."
    cd "$ETL_DIR" && uv sync 2>&1 | filter_output | tee -a "$LOG"
    log_ok "Dependências instaladas"
    echo "installed: $(date '+%Y-%m-%d %H:%M:%S')" > "$INSTALLED_FLAG"
    log_blank
}

# ── VALIDAÇÃO FINAL ──────────────────────────────────────────────────────────
run_validation() {
    log_banner "🔍  VALIDAÇÃO FINAL — lendo Neo4j"

    # Lê todos os labels diretamente do banco — independente de qualquer lista
    local raw
    raw=$(docker exec bracc-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
        "MATCH (n) RETURN labels(n)[0] as label, count(n) as total ORDER BY total DESC" \
        2>/dev/null | grep -v "^label\|^$\|rows\|ms")

    if [[ -z "$raw" ]]; then
        log_err "Não foi possível conectar ao Neo4j para validação"
        return
    fi

    local ok=0
    local total_nodes=0

    while IFS= read -r line; do
        # linha formato: "Label", 12345
        local label count
        label=$(echo "$line" | awk -F'"' '{print $2}')
        count=$(echo "$line" | awk -F',' '{print $2}' | tr -d ' ')
        [[ -z "$label" || -z "$count" ]] && continue
        log_info "  ✅  $label: $count nodes"
        ok=$((ok + 1))
        total_nodes=$((total_nodes + count))
    done <<< "$raw"

    log_blank
    log_info "Total: $ok labels | ~$total_nodes nodes no banco"
    log_blank
}

# ── DOCKER ───────────────────────────────────────────────────────────────────
start_docker() {
    log_banner "🐳  DOCKER"
    cd "$ROOT"
    docker compose up -d 2>&1 | filter_output | tee -a "$LOG"
    log_info "Aguardando Neo4j healthy..."
    local attempts=0
    while [[ $attempts -lt 30 ]]; do
        local status
        status=$(docker inspect --format='{{.State.Health.Status}}' bracc-neo4j 2>/dev/null || echo "starting")
        [[ "$status" == "healthy" ]] && { log_ok "Neo4j healthy"; return 0; }
        attempts=$((attempts + 1))
        log_info "Neo4j: $status ($attempts/30)..."
        sleep 10
    done
    log_err "Neo4j não ficou healthy — continuando mesmo assim"
}

# ── PNCP ─────────────────────────────────────────────────────────────────────
start_pncp_background() {
    log_banner "📥  PNCP — background"
    (
        while true; do
            cd "$ETL_DIR"
            uv run python scripts/download_pncp.py --output-dir "../data/pncp"
            echo "[PNCP] reiniciando em 30s..."
            sleep 30
        done >> "$LOG" 2>&1
    ) &
    log_ok "PNCP em background (PID $!)"
}

# ── DOWNLOAD ─────────────────────────────────────────────────────────────────
run_download() {
    local fonte="$1"
    local script="$ETL_DIR/scripts/download_${fonte}.py"
    [[ ! -f "$script" ]] && { log_skip "sem script de download — indo para importação"; return 0; }
    if [[ -d "$DATA_DIR/$fonte" ]] && [[ -n "$(ls -A "$DATA_DIR/$fonte" 2>/dev/null)" ]]; then
        log_skip "data/$fonte já existe — pulando download"
        return 0
    fi
    log_info "Baixando $fonte..."
    cd "$ETL_DIR"
    uv run python "scripts/download_${fonte}.py" --output-dir "../data/${fonte}" 2>&1 | filter_output | tee -a "$LOG" &
    CURRENT_PID=$!
    wait $CURRENT_PID
    local exit_code=$?
    CURRENT_PID=""
    # Verifica se realmente gerou arquivos — exit 0 não garante dados
    local file_count=0
    file_count=$(ls -A "$DATA_DIR/$fonte" 2>/dev/null | wc -l)
    if [[ $exit_code -eq 0 ]] && [[ "$file_count" -gt 0 ]]; then
        log_ok "Download concluído: $fonte ($file_count arquivo(s))"
    elif [[ $exit_code -eq 0 ]] && [[ "$file_count" -eq 0 ]]; then
        log_err "Download retornou OK mas nenhum arquivo gerado: $fonte — verifique URL/credencial"
        beep_error
        return 1
    else
        log_err "Download falhou: $fonte (código $exit_code)"
        beep_error
        return 1
    fi
}

# ── IMPORTAÇÃO ───────────────────────────────────────────────────────────────
run_import() {
    local fonte="$1"
    log_info "Importando $fonte..."
    cd "$ETL_DIR"
    uv run bracc-etl run --source "$fonte" --neo4j-password "$NEO4J_PASSWORD" --data-dir "$DATA_DIR" 2>&1 | filter_output | tee -a "$LOG" &
    CURRENT_PID=$!
    wait $CURRENT_PID
    local exit_code=$?
    CURRENT_PID=""
    if [[ $exit_code -eq 0 ]]; then
        mark_imported "$fonte"
        log_ok "Importação concluída: $fonte"
        beep
    else
        log_err "Importação falhou: $fonte (código $exit_code)"
        beep_error
        return 1
    fi
}

# ── MAIN ─────────────────────────────────────────────────────────────────────

# Processar argumentos
FORCE_FONTES=()
CUSTOM_QUEUE=()
i=1
while [[ $i -le $# ]]; do
    arg="${!i}"
    if [[ "$arg" == "--force" ]]; then
        FORCE_REIMPORT=1
        i=$((i + 1))
        while [[ $i -le $# ]]; do
            FORCE_FONTES+=("${!i}")
            i=$((i + 1))
        done
    else
        CUSTOM_QUEUE+=("$arg")
        i=$((i + 1))
    fi
done

clear
log_banner "🚀  BRACC-ETL ORQUESTRADOR  —  $(date '+%d/%m/%Y %H:%M')"
echo ""
echo "  Este programa baixa e importa todas as fontes de dados."
echo "  Fontes já importadas são puladas automaticamente."
echo ""
echo "  ┌────────────────────────────────────────────────────┐"
echo "  │  Ctrl+C encerra com segurança a qualquer momento   │"
echo "  │  O processo atual é finalizado antes de encerrar   │"
echo "  │  Você verá onde parou e como retomar               │"
echo "  └────────────────────────────────────────────────────┘"
echo ""
if [[ $FORCE_REIMPORT -eq 1 ]]; then
    echo "  ⚠️  Modo --force ativo para: ${FORCE_FONTES[*]}"
    echo ""
fi
read -rp "  Pressione ENTER para iniciar..." _
echo "" | tee -a "$LOG"

if [[ ! -f "$INSTALLED_FLAG" ]]; then
    run_setup
else
    log_info "✅ Setup já feito ($(grep 'installed:' "$INSTALLED_FLAG" | cut -d' ' -f2)) — pulando"
    log_blank
fi

start_docker

# Lê Neo4j e sincroniza .installed antes de qualquer fila
sync_neo4j_to_installed

start_pncp_background

if [[ ${#FORCE_FONTES[@]} -gt 0 ]] && [[ ${#CUSTOM_QUEUE[@]} -eq 0 ]]; then
    QUEUE=("${FORCE_FONTES[@]}")
    log_info "Fila --force: ${QUEUE[*]}"
elif [[ ${#CUSTOM_QUEUE[@]} -gt 0 ]]; then
    QUEUE=("${CUSTOM_QUEUE[@]}")
    log_info "Fila customizada: ${QUEUE[*]}"
else
    QUEUE=("${DEFAULT_QUEUE[@]}")
    log_info "Fila padrão: ${#QUEUE[@]} fontes"
fi
log_blank

TOTAL=${#QUEUE[@]}
COUNT=0
SKIPPED=0
ERRORS=0

for fonte in "${QUEUE[@]}"; do
    COUNT=$((COUNT + 1))
    CURRENT_FONTE="$fonte"
    log_section "[$COUNT/$TOTAL] $fonte"

    if is_skipped "$fonte"; then
        log_skip "na lista SKIP — ignorada"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Verificar se já foi importado (a menos que --force)
    force_this=0
    for ff in "${FORCE_FONTES[@]}"; do [[ "$ff" == "$fonte" ]] && force_this=1; done
    if [[ $FORCE_REIMPORT -eq 0 ]] || [[ $FORCE_REIMPORT -eq 1 && $force_this -eq 0 ]]; then
        if is_imported "$fonte"; then
            # Tem dados mais novos em disco que a última importação?
            _mtime=""
            _ts=""
            _date=""
            data_mtime=$(find "$DATA_DIR/$fonte" -type f -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1)
            installed_ts=$(date -d "$(grep "^imported: $fonte " "$INSTALLED_FLAG" | tail -1 | awk '{print $3, $4}')" +%s 2>/dev/null || echo "0")
            if [[ -n "$data_mtime" && "$data_mtime" -gt "${installed_ts:-0}" ]]; then
                local_date=$(grep "^imported: $fonte " "$INSTALLED_FLAG" | tail -1 | awk '{print $3, $4}')
                log_info "🔄 dados atualizados desde $local_date — reimportando"
            else
                local_date=$(grep "^imported: $fonte " "$INSTALLED_FLAG" | tail -1 | awk '{print $3, $4}')
                log_skip "já importado em $local_date — sem alteração"
                SKIPPED=$((SKIPPED + 1))
                continue
            fi
        fi
    fi

    run_download "$fonte" || { ERRORS=$((ERRORS + 1)); continue; }
    run_import "$fonte"   || { ERRORS=$((ERRORS + 1)); continue; }
    log_ok "[$COUNT/$TOTAL] $fonte concluído"
done

CURRENT_FONTE=""

# Validação final
run_validation

log_banner "✅  CONCLUÍDO  —  $(date '+%d/%m/%Y %H:%M')"
log_info "Total: $TOTAL  |  Puladas: $SKIPPED  |  Erros: $ERRORS"
log_blank
beep; beep