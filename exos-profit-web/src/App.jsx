import "./buttons.css";
import { useEffect, useMemo, useState } from "react";
import { api } from "./api/client";


function App() {
  const [me, setMe] = useState(null);
  const [jobs, setJobs] = useState([]);
  const [selectedSellerId, setSelectedSellerId] = useState("");
  const [limit, setLimit] = useState(10);
  const [dryRun, setDryRun] = useState(true);
  const [useCost, setUseCost] = useState(false);
  const [activeRunId, setActiveRunId] = useState("");
  const [activeJob, setActiveJob] = useState(null);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [billing, setBilling] = useState(null);
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadMessage, setUploadMessage] = useState("");
  const [activeFlow, setActiveFlow] = useState(null);
  const [minReceiveInfo, setMinReceiveInfo] = useState(null);
  const [skuStats, setSkuStats] = useState(null);



  const selectedSeller = useMemo(() => {
    return me?.sellers?.find(
      (s) => String(s.connected_seller_id) === String(selectedSellerId)
    );
  }, [me, selectedSellerId]);

  useEffect(() => {
    loadMe();
  }, []);

  useEffect(() => {
    if (selectedSellerId) loadJobs(selectedSellerId);
  }, [selectedSellerId]);

  useEffect(() => {
    if (!activeRunId) return;

    const timer = setInterval(async () => {
      try {
        const res = await api.get(`/run/status?run_id=${activeRunId}`);
        setActiveJob(res.data);

        if (res.data.status === "finished") {
          clearInterval(timer);

          await loadJobs(selectedSellerId);

          setTimeout(() => {
            loadJobs(selectedSellerId);
          }, 1000);

          // 👉 FINAL SIMPLES
          setTimeout(() => {
            setRunning(false);
            setActiveFlow(null);
          }, 500);
        }

        if (res.data.status === "error") {
          clearInterval(timer);

          setRunning(false);
          setActiveFlow(null);

          await loadJobs(selectedSellerId);
          setError(res.data.error || "Job finalizado com erro.");
        }
      } catch (err) {
        console.error(err);
        clearInterval(timer);
        setRunning(false);
      }
    }, 2500);

    return () => clearInterval(timer);
  }, [activeRunId, selectedSellerId]);

  async function loadMe() {
    try {
      setError("");

      const res = await api.get("/api/me");
      setMe(res.data);

      const firstSeller = res.data.sellers?.[0];

      if (firstSeller) {
        setSelectedSellerId(firstSeller.connected_seller_id);

        if (firstSeller.account_id) {
          const billingResp = await api.get(
            `/api/billing/summary?account_id=${firstSeller.account_id}`
          );
          setBilling(billingResp.data);

          await loadMinReceiveInfo(firstSeller.account_id);
          await loadSkuStats(firstSeller.account_id, firstSeller.connected_seller_id);
        }
      }
    } catch (err) {
      console.error(err);
      window.location.href = `${import.meta.env.VITE_API_URL}/login`;
    } finally {
      setLoading(false);
    }
  }

  async function loadMinReceiveInfo(accountId) {
    if (!accountId) return;

    try {
      const res = await api.get(`/account/min-receive?account_id=${accountId}`);
      setMinReceiveInfo(res.data);
    } catch (err) {
      console.error(err);
    }
  }

  async function loadJobs(sellerId) {
    try {
      const res = await api.get(
        `/api/jobs/recent?connected_seller_id=${sellerId}`
      );
      setJobs(res.data.jobs || []);
    } catch (err) {
      console.error(err);
      setError("Não foi possível carregar os últimas execuções.");
    }
  }



  async function runPipeline() {
    try {
      setRunning(true);
      setActiveFlow("full");
      setError("");
      setActiveJob(null);

      const params = new URLSearchParams({
        connected_seller_id: selectedSellerId,
        limit: String(limit || 0),
        dry_run: String(dryRun),
        use_cost: String(useCost),
      });

      const res = await api.get(`/run/full_async?${params.toString()}`);
      setActiveRunId(res.data.run_id);
      await loadJobs(selectedSellerId);
    } catch (err) {
      console.error(err);
      setRunning(false);
      setActiveFlow(null);
      const detail = err?.response?.data?.detail;
      setError(
        typeof detail === "string"
          ? detail
          : detail?.message || "Erro ao rodar pipeline."
      );
    }
  }

  async function loadSkuStats(accountId, sellerId) {
    try {
      const res = await api.get(
        `/account/sku-stats?account_id=${accountId}&connected_seller_id=${sellerId}`
      );
      setSkuStats(res.data);
    } catch (err) {
      console.error(err);
    }
  }

  async function runInventoryAsync() {
    try {
      setRunning(true);
      setActiveFlow("inventory");
      setError("");
      setActiveJob(null);

      const params = new URLSearchParams({
        connected_seller_id: selectedSellerId,
        limit: String(limit || 0),
      });

      const res = await api.get(`/run/inventory_async?${params.toString()}`);

      setActiveRunId(res.data.run_id);
      await loadJobs(selectedSellerId);
    } catch (err) {
      console.error(err);
      setRunning(false);
      setActiveFlow(null);

      const detail = err?.response?.data?.detail;

      setError(
        typeof detail === "string"
          ? detail
          : detail?.message || "Erro ao importar MLBs."
      );
    }
  }

  async function activateWinningCampaign() {
    try {
      setRunning(true);
      setError("");
      setActiveJob(null);
      setActiveFlow("campaign_winner");

      const params = new URLSearchParams({
        connected_seller_id: selectedSellerId,
        limit: String(limit || 0),
        dry_run: String(dryRun),
        use_cost: String(useCost),
      });

      const res = await api.get(`/run/campaign_winner_async?${params.toString()}`);

      setActiveRunId(res.data.run_id);
      await loadJobs(selectedSellerId);
    } catch (err) {
      console.error("CAMPAIGN WINNER ERROR:", err);
      console.error("RESPONSE:", err?.response?.data);

      setRunning(false);
      setActiveFlow(null);

      const detail = err?.response?.data?.detail;

      setError(
        typeof detail === "string"
          ? detail
          : detail?.message || "Erro ao ativar campanha vencedora."
      );
    }
  }

  async function handleLogout() {
    try {
      await api.post("/auth/logout");
    } catch (err) {
      console.error(err);
    } finally {
      localStorage.removeItem("token");
      localStorage.removeItem("user");
      window.location.href = "/login";
    }
  }

  function statusLabel(status) {
    return {
      queued: "Na fila",
      running: "Rodando",
      finished: "Finalizado",
      error: "Erro",
    }[status] || status || "-";
  }

  function getJobHeadline(job) {
    if (job.status === "queued") return "Job enfileirado.";
    if (job.status === "running") return "Executando job...";
    if (job.status === "error") return "Job finalizado com erro.";

    if (job.status === "finished") {
      if (job.job_type === "campaign_winner") {
        return "Campanha vencedora finalizada.";
      }

      return job.summary?.headline || "Finalizado com sucesso.";
    }

    return "-";
  }

  function getMinReceiveMessage(info, uploadMessage, skuStats) {
    if (uploadMessage) return uploadMessage;

    if (!info || typeof info.count !== "number") {
      return "Nenhum arquivo enviado nesta sessão.";
    }

    const count = info.count;

    if (count === 0) {
      return "Nenhum SKU com valor mínimo cadastrado.";
    }

    const updatedAt = info?.rows?.[0]?.updated_at;
    const dateText = updatedAt ? formatDate(updatedAt) : "-";

    const label = count === 1 ? "SKU" : "SKUs";

    // 👉 NOVO BLOCO (stats)
    let statsText = "";

    if (skuStats && skuStats.count_total_skus > 0) {
      const total = skuStats.count_total_skus;
      const withMin = skuStats.count_with_min;

      const pct = Math.round((withMin / total) * 100);

      statsText = ` • ${withMin} com valor mínimo definido (${pct}%)`;
    }

    return `${count} ${label} cadastrados${statsText}. Última atualização: ${dateText}.`;
  }

  function formatDate(dateStr) {
    if (!dateStr) return "-";

    const d = new Date(dateStr);

    const day = String(d.getDate()).padStart(2, "0");
    const month = String(d.getMonth() + 1).padStart(2, "0");
    const hour = String(d.getHours()).padStart(2, "0");
    const minute = String(d.getMinutes()).padStart(2, "0");

    return `${day}/${month} (${hour}h${minute})`;
  }

  const latestJob = jobs?.[0];

  if (loading) {
    return <div style={styles.loading}>Carregando Exos Profit...</div>;
  }

  async function uploadMinReceiveFile() {
    if (!file || !selectedSeller?.account_id) return;

    try {
      setUploading(true);
      setUploadMessage(`Arquivo enviado com sucesso. ${res.data.rows_saved} SKUs salvos.`);
      setFile(null);
      await loadMinReceiveInfo(selectedSeller.account_id);

      const formData = new FormData();
      formData.append("file", file);

      const res = await api.post(
        `/account/min-receive/upload?account_id=${selectedSeller.account_id}`,
        formData,
        {
          headers: {
            "Content-Type": "multipart/form-data",
          },
        }
      );

      setUploadMessage(`Arquivo enviado com sucesso. ${res.data.rows_saved} SKUs salvos.`);
      setFile(null);
    } catch (err) {
      console.error("UPLOAD ERROR:", err);
      console.error("UPLOAD RESPONSE:", err?.response?.data);

      const detail = err?.response?.data?.detail;

      if (typeof detail === "string") {
        setUploadMessage(detail);
      } else if (Array.isArray(detail)) {
        setUploadMessage(detail.map((d) => d.msg).join(" | "));
      } else if (detail?.message) {
        setUploadMessage(detail.message);
      } else {
        setUploadMessage("Erro ao enviar arquivo. Veja o console e o terminal do backend.");
      }
    } finally {
      setUploading(false);
    }
  }

  return (
    <div style={styles.shell}>

      <main style={styles.main}>
        <header style={styles.topbar}>
          <div>
            <h1 style={styles.title}>Exosss Tools</h1>
            <p style={styles.subtitle}>
              Automação de campanhas do Mercado Livre.
            </p>
          </div>

          <div style={styles.userBox}>
            <strong style={styles.userEmail}>
              {me?.user?.email}
            </strong>

            <button style={styles.logoutButton} onClick={handleLogout}>
              Sair
            </button>
          </div>
        </header>

        {error && <div style={styles.error}>{error}</div>}

        <section style={styles.topCardsGrid}>
          <section style={styles.connectedCard}>
            <div style={styles.connectedIcon}>✓</div>

            <div>
              <h2 style={styles.connectedTitle}>Conta conectada</h2>

              <div style={styles.connectedDetails}>
                <span><strong>Conta:</strong> {selectedSeller?.seller_nickname || "-"}</span>
                <span><strong>User ID:</strong> {selectedSeller?.ml_user_id || "-"}</span>
                <span><strong>Plataforma:</strong> {selectedSeller?.site_id || "-"}</span>
                <span><strong>Status:</strong> {selectedSeller?.status || "-"}</span>
              </div>
            </div>
          </section>

          <section style={styles.planCard}>

            <div style={styles.planHeader}>
              <span style={styles.planBadge}><strong>TRIALING</strong></span>

              <div style={styles.planLineMain}>
                <span style={styles.planLabel}>Plano atual:</span>
                <strong>Trial 10 dias</strong>
              </div>
            </div>

            <div style={styles.planContent}>

              <div style={styles.planLineHighlight}>
                {billing?.days_remaining ?? "-"} dias restantes no período
              </div>

              <div style={styles.planLine}>
                Limite por execução: <strong>{billing?.subscription?.mlb_limit || 10} MLBs</strong>
              </div>

              <div style={styles.planLine}>
                Execuções restantes hoje: <strong>{billing?.executions_remaining_today ?? "-"}</strong>
              </div>

            </div>
          </section>
        </section>

        <section style={styles.contentGrid}>
          <div style={styles.leftColumn}>

            <section style={styles.cardHighlight}>
              <div style={styles.cardHeader}>
                <div>
                  <h2>Executar otimização</h2>
                </div>
              </div>

              <div style={styles.formGrid}>
                <div>
                  <div style={styles.inlineInfo}>
                    <span style={styles.inlineLabel}>Escopo de execução:</span>
                    <span style={styles.inlineValue}>{limit}</span>
                  </div>
                </div>

                <label style={styles.check}>
                  <input
                    type="checkbox"
                    checked={dryRun}
                    onChange={(e) => setDryRun(e.target.checked)}
                    style={{ margin: 0 }} // 👈 remove espaço estranho
                  />
                  Simulação (Não efetiva no Mercado Livre)
                </label>

                <label style={styles.check}>
                  <input
                    type="checkbox"
                    checked={useCost}
                    onChange={(e) => setUseCost(e.target.checked)}
                    style={{ margin: 0 }} // 👈 remove espaço estranho
                  />
                  Usar custo
                </label>
              </div>
              
              <div style={styles.secondaryActions}>
               <button
                  className="secondary-button"
                  onClick={runInventoryAsync}
                  disabled={running || activeFlow !== null || !selectedSellerId}
                >
                  {activeFlow === "inventory" ? "Executando..." : "Importar MLBs"}
                </button>

                <button
                  className="secondary-button"
                  onClick={activateWinningCampaign}
                  disabled={running || activeFlow !== null || !selectedSellerId}
                >
                  {activeFlow === "campaign_winner" ? "Executando..." : "Ativar campanha vencedora"}
                </button>
              </div>

              <button
                style={styles.primaryButton}
                onClick={runPipeline}
                disabled={running || activeFlow !== null || !selectedSellerId}
              >
                {activeFlow === "full" ? "Otimização em execução..." : "Rodar otimização completa"}
              </button>

              {running && activeJob && (
                <div style={styles.activeJobRow}>
                  <span
                    style={{
                      ...styles.badgeBase,
                      ...getBadgeStyle(activeJob.status),
                    }}
                  >
                    {statusLabel(activeJob.status)}
                  </span>

                  <span style={styles.activeJobText}>
                    {activeJob.summary?.headline || "Processando..."}
                  </span>
                </div>
              )}
            </section>
          </div>
          <div style={styles.rightColumn}>
            <section style={styles.minReceiveCard}>
              <div style={styles.minReceiveHeader}>
                <div>
                  <h2 style={styles.sectionTitle}>Valor mínimo a receber por SKU</h2>
                  <p style={styles.sectionDescription}>
                    Baixe o template, preencha <strong>sku</strong> e{" "}
                    <strong>valor minímo à receber </strong>por tipo de anúnico e envie o arquivo.
                  </p>
                </div>
              </div>

              <div style={styles.uploadCompactRow}>
                <a
                  href={`${import.meta.env.VITE_API_URL}/template/sku-min-receber.csv?account_id=${selectedSeller?.account_id}&connected_seller_id=${selectedSellerId}`}
                  target="_blank"
                  style={styles.templateButtonSmall}
                >
                  Baixar template
                </a>

                <input
                  id="min-receive-file"
                  type="file"
                  accept=".csv,.xlsx"
                  style={styles.fileInputHidden}
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                />

                <label htmlFor="min-receive-file" style={styles.fileButtonSmall}>
                  Escolher arquivo
                </label>

                <span style={styles.fileName}>
                  {file ? file.name : "Nenhum arquivo escolhido"}
                </span>

                <button
                  style={{
                    ...styles.uploadButtonSmall,
                    opacity: !file || uploading ? 0.5 : 1,
                    cursor: !file || uploading ? "not-allowed" : "pointer",
                  }}
                  disabled={!file || uploading}
                  onClick={uploadMinReceiveFile}
                >
                  {uploading ? "Enviando..." : "Enviar"}
                </button>
              </div>

              <p style={styles.mutedSmall}>
                {getMinReceiveMessage(minReceiveInfo, uploadMessage, skuStats)}
              </p>
            </section>

            <section style={styles.card}>
              <div style={styles.cardHeader}>
                <h2 style={styles.sectionTitle}>Últimas execuções</h2>
              </div>

              <div style={styles.jobsList}>
                {jobs.map((job) => (
                  <div key={job.run_id} style={styles.jobRow}>
      
                    {/* coluna esquerda */}
                    <div style={styles.jobLeft}>
                      <span
                        style={{
                          ...styles.badgeBase,
                          ...getBadgeStyle(job.status),
                        }}
                      >
                        {statusLabel(job.status)}
                      </span>
                    </div>

                    {/* coluna direita */}
                    <div style={styles.jobRight}>
                      
                      {/* linha 1 */}
                      <div style={styles.jobHeadline}>
                        {getJobHeadline(job)}
                      </div>

                      {/* linha 2 */}
                      <div style={styles.jobBottom}>
                        <span>{formatDate(job.created_at)}</span>

                        <span style={styles.dot}>•</span>

                        {job.has_csv ? (
                          <>
                            <a
                              style={styles.link}
                              href={`${import.meta.env.VITE_API_URL}/download/csv?run_id=${job.run_id}`}
                              target="_blank"
                            >
                              Final
                            </a>

                            {job.has_csv_detailed && (
                              <>
                                <span style={styles.dot}>•</span>

                                <a
                                  style={styles.linkSecondary}
                                  href={`${import.meta.env.VITE_API_URL}/download/csv_detailed?run_id=${job.run_id}`}
                                  target="_blank"
                                >
                                  Detalhado
                                </a>
                              </>
                            )}
                          </>
                        ) : (
                          <span style={styles.muted}>Sem CSV</span>
                        )}
                      </div>

                    </div>
                  </div>
                ))}
              </div>
            </section>
          </div>
        </section>

      </main>
    </div>
  );
}

function Metric({ title, value }) {
  return (
    <div style={styles.metric}>
      <span>{title}</span>
      <strong>{value || "-"}</strong>
    </div>
  );
}

function Info({ label, value }) {
  return (
    <div style={styles.info}>
      <span>{label}</span>
      <strong>{value || "-"}</strong>
    </div>
  );
}

function getBadgeStyle(status) {
  const s = (status || "").toLowerCase();

  if (s.includes("finish") || s.includes("success")) {
    return {
      background: "rgba(34,197,94,.15)",
      color: "#4ade80",
    };
  }

  if (s.includes("running") || s.includes("processing")) {
    return {
      background: "rgba(59,130,246,.15)",
      color: "#60a5fa",
    };
  }

  if (s.includes("error") || s.includes("fail")) {
    return {
      background: "rgba(239,68,68,.15)",
      color: "#f87171",
    };
  }

  // fallback
  return {
    background: "rgba(148,163,184,.15)",
    color: "#94a3b8",
  };
}

const styles = {
   shell: {
    minHeight: "100vh",
    background: "#020617",
    color: "#e5e7eb",
    fontFamily: "Inter, Arial, sans-serif",
  },
  loading: {
    minHeight: "100vh",
    background: "#020617",
    color: "#e5e7eb",
    display: "grid",
    placeItems: "center",
    fontFamily: "Arial",
  },
  sidebar: {
    borderRight: "1px solid rgba(255,255,255,.08)",
    padding: 24,
    display: "flex",
    flexDirection: "column",
    gap: 28,
    background: "rgba(2,6,23,.82)",
  },
  logo: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    marginBottom: 10,
  },
  logoMark: {
    width: 42,
    height: 42,
    borderRadius: 14,
    background: "linear-gradient(135deg,#2FD4C7,#38bdf8)",
    color: "#022c22",
    display: "grid",
    placeItems: "center",
    fontWeight: 900,
    fontSize: 22,
  },
  nav: {
    display: "grid",
    gap: 8,
  },
  navActive: {
    textAlign: "left",
    border: 0,
    borderRadius: 12,
    padding: "12px 14px",
    background: "rgba(47,212,199,.14)",
    color: "#67e8f9",
    fontWeight: 800,
  },
  navItem: {
    textAlign: "left",
    border: 0,
    borderRadius: 12,
    padding: "12px 14px",
    background: "transparent",
    color: "#94a3b8",
    fontWeight: 600,
  },
  sidebarFooter: {
    marginTop: "auto",
    background: "rgba(15,23,42,.9)",
    border: "1px solid rgba(255,255,255,.08)",
    borderRadius: 16,
    padding: 14,
    display: "grid",
    gap: 6,
  },
  main: {
    padding: "32px 40px",
    maxWidth: 1120,
    margin: "0 auto",
  },
  topbar: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 24,
  },

  title: {
    margin: 0,
    fontSize: 36,
  },
  subtitle: {
    margin: "6px 0 0",
    color: "#94a3b8",
  },

 userBox: {
    display: "flex",
    alignItems: "center",
    gap: 12,
  },

  userEmail: {
    fontSize: 14,
    color: "#94a3b8",
    fontWeight: 600,
  },

  metricsGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
    gap: 12,
    marginBottom: 18,
  },

  metric: {
    background: "rgba(15,23,42,.72)",
    border: "1px solid rgba(255,255,255,.07)",
    borderRadius: 14,
    padding: "12px 14px",
    display: "grid",
    gap: 4,
    minHeight: 72,
  },

  contentGrid: {
    display: "grid",
    gridTemplateColumns: "360px 1fr",
    gap: 20,
    alignItems: "start",
  },
  connectedCard: {
    display: "flex",
    alignItems: "flex-start",
    gap: 10,
    background: "linear-gradient(180deg, rgba(47,212,199,.13), rgba(15,23,42,.86))",
    border: "1px solid rgba(47,212,199,.28)",
    borderRadius: 14,
    padding: "12px 14px",
  },
  connectedIcon: {
      width: 30,
      height: 30,
      borderRadius: 8,
      background: "#22c55e",
      display: "grid",
      placeItems: "center",
      fontWeight: "bold",
    },

  connectedTitle: {
    margin: "0 0 6px",
    fontSize: 18,
    textAlign: "left",
  },
  connectedDetails: {
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-start",
    gap: 2,
    color: "#cbd5e1",
    fontSize: 14,
    lineHeight: 1.3,
  },

  leftColumn: {
    display: "grid",
    gap: 20,
  },

  rightColumn: {
    display: "grid",
    gap: 15,
  },

  card: {
    background: "linear-gradient(180deg, rgba(15,23,42,.9), rgba(2,6,23,.9))",
    border: "1px solid rgba(255,255,255,.08)",
    borderRadius: 18,
    padding: 18,
    boxShadow: "0 18px 60px rgba(0,0,0,.18)",
  },
  cardHighlight: {
    background:
      "linear-gradient(180deg, rgba(47,212,199,.08), rgba(15,23,42,.88))",
    border: "1px solid rgba(47,212,199,.18)",
    borderRadius: 18,
    padding: 18,
  },
  cardHeader: {
    marginBottom: 18,
  },
  label: {
    display: "block",
    color: "#94a3b8",
    fontSize: 13,
    marginBottom: 8,
  },
  input: {
    width: "100%",
    boxSizing: "border-box",
    padding: "13px 14px",
    borderRadius: 14,
    border: "1px solid rgba(255,255,255,.12)",
    background: "#0f172a",
    color: "#e5e7eb",
    fontSize: 15,
  },
  formGrid: {
    display: "grid",
    gap: 6,
    marginBottom: 12,
  },
  check: {
    display: "flex",
    alignItems: "center",
    gap: 2,
    color: "#cbd5e1",
    fontSize: 15,        // 👈 menor
    lineHeight: 1.2,
    margin: 0,
  },
  primaryButton: {
    width: "100%",
    border: 0,
    borderRadius: 15,
    padding: "15px 18px",
    background: "linear-gradient(135deg,#2FD4C7,#1CB3A6)",
    color: "#022c22",
    fontWeight: 900,
    cursor: "pointer",
    fontSize: 15,
  },

  secondaryButton: {
    flex: 1,
    borderRadius: 15, // 👈 igual ao principal
    padding: "8px 10px",
    background: "rgba(255,255,255,.04)",
    border: "1px solid rgba(255,255,255,.08)",
    color: "#cbd5e1",
    fontWeight: 600,
    cursor: "pointer",
    fontSize: 14,
    transition: "all .15s ease", // 👈 ESSENCIAL
    boxShadow: "0 2px 6px rgba(0,0,0,.15)", // 👈 NOVO
  },
  

  secondaryActions: {
    display: "flex",
    gap: 8,
    marginTop: 8,
    marginBottom: 14, // 👈 aumenta espaço
  },

  infoList: {
    display: "grid",
    gap: 10,
    marginTop: 16,
  },
  info: {
    background: "rgba(2,6,23,.55)",
    borderRadius: 14,
    padding: 14,
    display: "flex",
    justifyContent: "space-between",
    gap: 10,
  },

  infoField: {
    padding: "13px 14px",
    borderRadius: 14,
    background: "rgba(255,255,255,.04)",
    border: "1px solid rgba(255,255,255,.06)",
    color: "#cbd5e1",
    fontSize: 15,
  },

  inlineInfo: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    marginBottom: 12,
  },

  inlineLabel: {
    color: "#94a3b8",
    fontSize: 14,
  },

  inlineValue: {
    color: "#e2e8f0",
    fontWeight: 700,
    fontSize: 14,
  },

  jobsList: {
    display: "flex",
    flexDirection: "column",
    gap: 8,
  },

  jobRow: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "14px",
    borderRadius: 12,
    background: "rgba(255,255,255,.03)",
    border: "1px solid rgba(255,255,255,.05)",
  },

  jobLeft: {
    width: 115,
    flexShrink: 0,
  },

  jobRight: {
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-start",
    justifyContent: "center",
    gap: 4,
    flex: 1,
    textAlign: "left",
  },

  jobTop: {
    display: "flex",
    alignItems: "center",
    gap: 10,
  },

  jobBottom: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    fontSize: 12,
    color: "#64748b",
  },

  dot: {
    opacity: 0.6,
  },


  jobMain: {
    display: "flex",
    flexDirection: "column",
    gap: 2,
  },

  jobTitle: {
    fontSize: 14,
    fontWeight: 600,
    color: "#e5e7eb",
  },

  jobDescription: {
    fontSize: 12,
    color: "#94a3b8",
    lineHeight: 1.3,
  },


  jobMeta: {
    display: "contents",
  },
  badge: {
    padding: "5px 10px",
    borderRadius: 999,
    background: "rgba(34,197,94,.15)",
    color: "#4ade80",
    fontWeight: 800,
    fontSize: 12,
  },

  badgeBase: {
    padding: "2px 8px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 600,
    textAlign: "center",
    minWidth: 90,
    display: "inline-block",
  },

  link: {
    color: "#a5b4fc",
    fontWeight: 800,
  },

  linkSecondary: {
    color: "#94a3b8",
    fontWeight: 600,
    fontSize: 12,
  },

  muted: {
    color: "#64748b",
  },
  activeJob: {
    marginTop: 16,
    background: "rgba(2,6,23,.55)",
    borderRadius: 14,
    padding: 14,
    display: "grid",
    gap: 6,
  },
  error: {
    background: "rgba(239,68,68,.14)",
    border: "1px solid rgba(239,68,68,.28)",
    color: "#fecaca",
    padding: 14,
    borderRadius: 14,
    marginBottom: 18,
  },

  topCardsGrid: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: 10,
    marginBottom: 15  },


   planCard: {
    background: "rgba(15,23,42,.78)",
    border: "1px solid rgba(255,255,255,.08)",
    borderRadius: 14,
    padding: "14px",
  },

  planLabel: {
    color: "#94a3b8",
    fontSize: 18,
  },

  planTitle: {
    margin: "0 0 8px",
    fontSize: 22,
    textAlign: "left",
    width: "100%",
  },

  planHeader: {
    display: "grid",
    gridTemplateColumns: "auto 1fr",
    gap: 8,
    alignItems: "center",
    marginBottom: 1, // 👈 igual ao espaçamento interno
  },

  planBadge: {
      padding: "4px 10px",
      borderRadius: 999,
      background: "rgba(59,130,246,.18)",
      border: "1px solid rgba(59,130,246,.45)",
      fontSize: 11,
    },


  planContent: {
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-start",
    gap: 2,              // 👈 menor ainda
    fontSize: 14,
    lineHeight: 1.3,
    marginTop: 1,
    marginLeft: "calc(80px + 8px)", // badge + gap
  },

  planDetails: {
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-start",
    textAlign: "left",
    gap: 3,
    color: "#cbd5e1",
    fontSize: 14,
    lineHeight: 1.3,
  },

  planRemaining: {
    color: "#86efac",
    fontWeight: 700,
  },

  planLineMain: {
    fontSize: 18,
    display: "flex",
    alignItems: "center",
    gap: 6,
    lineHeight: 1.3,
  },

  planLabel: {
    color: "#94a3b8",
  },

  planLine: {
    fontSize: 14,
    color: "#cbd5e1",
    margin: 0,
  },

  planLineHighlight: {
    fontSize: 14,
    color: "#86efac",
    fontWeight: 600,
    margin: 0,
  },

  minReceiveCard: {
    background: "linear-gradient(180deg, rgba(15,23,42,.9), rgba(30,41,59,.72))",
    border: "1px solid rgba(255,255,255,.08)",
    borderRadius: 18,
    padding: 18,
  },

  minReceiveHeader: {
    marginBottom: 14,
  },

  sectionTitle: {
    margin: "0 0 6px",
    fontSize: 20,
    textAlign: "left",
  },

  sectionDescription: {
    margin: 0,
    color: "#94a3b8",
    fontSize: 13,
    lineHeight: 1.35,
    textAlign: "left",
  },

  uploadCompactRow: {
    display: "grid",
    gridTemplateColumns: "140px 150px minmax(0, 1fr) 100px",
    gap: 10,
    alignItems: "center",
  },

  templateButtonSmall: {
    textAlign: "center",
    textDecoration: "none",
    borderRadius: 12,
    padding: "9px 10px",
    background: "rgba(47,212,199,.10)",
    border: "1px solid rgba(47,212,199,.20)",
    color: "#67e8f9",
    fontWeight: 700,
    fontSize: 13,
    whiteSpace: "nowrap",
  },

  fileInputHidden: {
    display: "none",
  },

  fileButtonSmall: {
    textAlign: "center",
    borderRadius: 12,
    padding: "9px 10px",
    background: "rgba(255,255,255,.05)",
    border: "1px solid rgba(255,255,255,.10)",
    color: "#e5e7eb",
    fontWeight: 600,
    fontSize: 13,
    cursor: "pointer",
    whiteSpace: "nowrap",
  },

  fileName: {
    color: "#94a3b8",
    fontSize: 13,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },

  uploadButtonSmall: {
    border: 0,
    borderRadius: 12,
    padding: "10px 12px",
    background: "linear-gradient(135deg,#3b82f6,#2563eb)",
    color: "#eff6ff",
    fontWeight: 800,
    fontSize: 13,
    cursor: "pointer",
  },

  mutedSmall: {
    margin: "10px 0 0",
    color: "#ffffff",
    fontSize: 12,
  },

  activeJobCard: {
    marginTop: 12,
    padding: "10px 12px",
    borderRadius: 12,
    background: "rgba(255,255,255,.03)",
    border: "1px solid rgba(255,255,255,.06)",
    display: "flex",
    flexDirection: "column",
    gap: 4,
  },

  activeJobTitle: {
    fontSize: 14,
    fontWeight: 600,
    color: "#e5e7eb",
  },

  activeJobMeta: {
    display: "grid",
    gridTemplateColumns: "auto auto",
    gap: "16px",
    fontSize: 12,
    color: "#94a3b8",
  },

  activeJobRow: {
    marginTop: 12,
    display: "flex",
    alignItems: "center",
    gap: 10,
    padding: "10px 12px",
    borderRadius: 12,
    background: "rgba(255,255,255,.03)",
    border: "1px solid rgba(255,255,255,.06)",
  },

  activeJobText: {
    fontSize: 13,
    color: "#cbd5e1",
    lineHeight: 1.3,
    
  },

  activeJobStep: {
    color: "#94a3b8",
    fontSize: 12,
  },

  jobDate: {
    fontSize: 16,
    color: "#ffffff",
  },

  jobHeadline: {
    fontSize: 15,
    fontWeight: 600,
    color: "#ffffff",
    lineHeight: 1.3,
  },

 userBox: {
  display: "flex",
  alignItems: "center",
  gap: 12,
},

userInfo: {
  display: "flex",
  flexDirection: "column",
  alignItems: "flex-end", // 👈 importante
  fontSize: 13,
  color: "#cbd5e1",
},

logoutButton: {
  background: "rgba(255,255,255,.04)",
  border: "1px solid rgba(255,255,255,.10)",
  color: "#cbd5e1",
  padding: "7px 12px",
  borderRadius: 10,
  cursor: "pointer",
  fontSize: 12,
  fontWeight: 700,
},

  
};

export default App;