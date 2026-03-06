import React, { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend
} from "recharts";

const api = {
  async getBuildings() {
    const res = await fetch("/buildings");
    if (!res.ok) throw new Error("Failed to fetch buildings");
    return res.json();
  },
  async getDevices(buildingId) {
    if (buildingId === "all") return [];
    const res = await fetch(`/buildings/${buildingId}/devices`);
    if (!res.ok) throw new Error("Failed to fetch devices");
    return res.json();
  },
  async getLatest(deviceId) {
    if (deviceId === "all") return [];
    const res = await fetch(`/devices/${deviceId}/latest`);
    if (!res.ok) throw new Error("Failed to fetch latest measurements");
    return res.json();
  },
  async getRecent(deviceId, metric, limit, offset) {
    if (deviceId === "all") return [];
    const params = new URLSearchParams({ metric, limit: String(limit), offset: String(offset) });
    const res = await fetch(`/devices/${deviceId}/recent?${params.toString()}`);
    if (!res.ok) throw new Error("Failed to fetch recent measurements");
    return res.json();
  },
  async getTimeseries(deviceId, metric, start, end, buildingId) {
    if (buildingId === "all" || deviceId === "all") {
      const params = new URLSearchParams({
        building_id: buildingId || "all",
        device_id: deviceId || "all",
        metric: metric || "energy_kwh_total"
      });
      if (start) params.append("start", start.toISOString());
      if (end) params.append("end", end.toISOString());
      const res = await fetch(`/timeseries/aggregated?${params.toString()}`);
      if (!res.ok) throw new Error("Failed to fetch aggregated timeseries");
      return res.json();
    }
    const params = new URLSearchParams({ device_id: deviceId, metric });
    if (start) params.append("start", start.toISOString());
    if (end) params.append("end", end.toISOString());
    const res = await fetch(`/timeseries?${params.toString()}`);
    if (!res.ok) throw new Error("Failed to fetch timeseries");
    return res.json();
  }
};

function formatQualityFlags(m) {
  const parts = [];
  if (m.is_normal) parts.push("Normalized");
  if (m.is_reset) parts.push("Reset");
  if (m.is_duplicate) parts.push("Duplicate");
  if (m.is_late) parts.push("Late");
  if (m.is_bad) parts.push("Bad");
  return parts.length ? parts.join(", ") : "";
}

function metricDisplayName(metric, unit) {
  if (metric === "energy_kwh_total") return `Energy Total (${unit})`;
  return `${metric} (${unit})`;
}

const PREFIXES = [
  { value: 1, label: "kWh" },
  { value: 1e3, label: "MWh" },
  { value: 1e6, label: "GWh" }
];

const RECORDS_PER_PAGE = 5;

function toDateStr(d) {
  return d.toISOString().slice(0, 10);
}

function startOfDay(str) {
  if (!str) return null;
  return new Date(str + "T00:00:00.000Z");
}

function endOfDay(str) {
  if (!str) return null;
  return new Date(str + "T23:59:59.999Z");
}

function App() {
  const [buildings, setBuildings] = useState([]);
  const [selectedBuildingId, setSelectedBuildingId] = useState("");
  const [devices, setDevices] = useState([]);
  const [selectedDeviceId, setSelectedDeviceId] = useState("");
  const [latest, setLatest] = useState([]);
  const [recentRecords, setRecentRecords] = useState([]);
  const [recentPage, setRecentPage] = useState(0);
  const [selectedMetric, setSelectedMetric] = useState("");
  const [metricMode, setMetricMode] = useState("raw");
  const [dateStart, setDateStart] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    return toDateStr(d);
  });
  const [dateEnd, setDateEnd] = useState(() => toDateStr(new Date()));
  const [timeseries, setTimeseries] = useState([]);
  const [aggregatedData, setAggregatedData] = useState([]);
  const [valuePrefix, setValuePrefix] = useState(1);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const [allTime, setAllTime] = useState(false);
  const [activePreset, setActivePreset] = useState("7d");

  const getTimeRangeDates = () => {
    if (allTime || !dateStart || !dateEnd) return { start: null, end: null };
    return { start: startOfDay(dateStart), end: endOfDay(dateEnd) };
  };

  const applyPreset = (preset) => {
    setActivePreset(preset);
    setAllTime(preset === "all");
    if (preset === "7d") {
      const d = new Date();
      setDateEnd(toDateStr(d));
      d.setDate(d.getDate() - 7);
      setDateStart(toDateStr(d));
    } else if (preset === "30d") {
      const d = new Date();
      setDateEnd(toDateStr(d));
      d.setDate(d.getDate() - 30);
      setDateStart(toDateStr(d));
    }
  };

  const onDateChange = (which, value) => {
    setActivePreset(null);
    if (which === "start") setDateStart(value);
    else setDateEnd(value);
  };

  useEffect(() => {
    api
      .getBuildings()
      .then((data) => {
        setBuildings(data);
        if (data.length > 0) {
          setSelectedBuildingId(data[0].id);
        }
      })
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    if (!selectedBuildingId) return;
    api
      .getDevices(selectedBuildingId)
      .then((data) => {
        setDevices(data);
        if (data.length > 0) {
          setSelectedDeviceId(data[0].id);
        } else {
          setSelectedDeviceId(selectedBuildingId === "all" ? "all" : "");
          setLatest([]);
          setRecentRecords([]);
          setTimeseries([]);
          if (selectedBuildingId === "all") setSelectedMetric("energy_kwh_total");
        }
      })
      .catch((e) => setError(e.message));
  }, [selectedBuildingId]);

  useEffect(() => {
    if (!selectedDeviceId || selectedDeviceId === "all") {
      setLatest([]);
      setRecentRecords([]);
      return;
    }
    setLoading(true);
    api
      .getLatest(selectedDeviceId)
      .then((data) => {
        setLatest(data);
        if (data.length > 0) {
          setSelectedMetric(data[0].metric);
        } else {
          setSelectedMetric("");
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [selectedDeviceId]);

  useEffect(() => {
    if (!selectedDeviceId || !selectedMetric) {
      setRecentRecords([]);
      setRecentPage(0);
      return;
    }
    if (selectedDeviceId === "all") return;
    api
      .getRecent(selectedDeviceId, selectedMetric, 100, 0)
      .then((data) => {
        setRecentRecords(data);
        setRecentPage(0);
      })
      .catch(() => setRecentRecords([]));
  }, [selectedDeviceId, selectedMetric]);

  useEffect(() => {
    if (!selectedMetric) {
      setTimeseries([]);
      setAggregatedData([]);
      return;
    }
    const useAggregated = selectedBuildingId === "all" || selectedDeviceId === "all";
    const { start, end } = getTimeRangeDates();
    if (!useAggregated && selectedDeviceId) {
      setLoading(true);
      api
        .getTimeseries(selectedDeviceId, selectedMetric, start, end)
        .then((data) => {
          setTimeseries(data);
          setAggregatedData([]);
        })
        .catch((e) => setError(e.message))
        .finally(() => setLoading(false));
    } else if (useAggregated) {
      setLoading(true);
      api
        .getTimeseries("all", selectedMetric, start, end, selectedBuildingId || "all")
        .then((data) => {
          setAggregatedData(Array.isArray(data) ? data : []);
          setTimeseries([]);
        })
        .catch((e) => setError(e.message))
        .finally(() => setLoading(false));
    } else {
      setTimeseries([]);
      setAggregatedData([]);
    }
  }, [selectedDeviceId, selectedMetric, selectedBuildingId, dateStart, dateEnd, allTime]);

  const metricOptions = useMemo(() => {
    const opts = latest.map((m) => ({
      value: m.metric,
      label: metricDisplayName(m.metric, m.unit)
    }));
    if (opts.length === 0 && (selectedBuildingId === "all" || selectedDeviceId === "all")) {
      return [{ value: "energy_kwh_total", label: "Energy Total (kWh)" }];
    }
    return opts;
  }, [latest, selectedBuildingId, selectedDeviceId]);

  const paginatedRecent = useMemo(() => {
    const start = recentPage * RECORDS_PER_PAGE;
    return recentRecords.slice(start, start + RECORDS_PER_PAGE);
  }, [recentRecords, recentPage]);

  const totalRecentPages = Math.max(1, Math.ceil(recentRecords.length / RECORDS_PER_PAGE));
  const canGoNext = recentPage > 0;
  const canGoPrev = recentPage < totalRecentPages - 1;

  const chartData = useMemo(() => {
    if (aggregatedData.length > 0) {
      const byTs = {};
      aggregatedData.forEach((r) => {
        const ts = r.ts;
        const label = r.label || "Total";
        if (!byTs[ts]) byTs[ts] = { tsLabel: new Date(ts).toLocaleTimeString(), ts };
        const key = metricMode === "delta" ? "delta" : "value";
        byTs[ts][label] = ((r[key] ?? 0) / valuePrefix);
      });
      return Object.values(byTs).sort((a, b) => new Date(a.ts) - new Date(b.ts));
    }
    return timeseries
      .filter((p) => !p.is_bad)
      .map((p) => ({
        ...p,
        tsLabel: new Date(p.ts).toLocaleTimeString(),
        value: (p.value || 0) / valuePrefix,
        delta: (p.delta ?? 0) / valuePrefix
      }));
  }, [timeseries, aggregatedData, valuePrefix, metricMode]);

  const CHART_COLORS = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#4f46e5", "#059669"];

  const chartsByBuilding = useMemo(() => {
    if (selectedBuildingId !== "all" || aggregatedData.length === 0) return [];
    const key = metricMode === "delta" ? "delta" : "value";
    const byBuilding = {};
    aggregatedData.forEach((r) => {
      const label = r.label || "Total";
      if (!byBuilding[label]) byBuilding[label] = [];
      byBuilding[label].push({
        ts: r.ts,
        tsLabel: new Date(r.ts).toLocaleTimeString(),
        building: label,
        [key]: (r[key] ?? 0) / valuePrefix
      });
    });
    return Object.entries(byBuilding).map(([name, data]) => ({
      buildingName: name,
      data: data.sort((a, b) => new Date(a.ts) - new Date(b.ts))
    }));
  }, [selectedBuildingId, aggregatedData, metricMode, valuePrefix]);

  const chartDataKey = metricMode === "delta" ? "delta" : "value";
  const yAxisLabel = useMemo(() => {
    const unit = PREFIXES.find((p) => p.value === valuePrefix)?.label || "kWh";
    return metricMode === "delta" ? `Delta (${unit})` : `Value (${unit})`;
  }, [valuePrefix, metricMode]);

  return (
    <div
      style={{
        fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, sans-serif",
        padding: "1.5rem",
        maxWidth: "960px",
        margin: "0 auto"
      }}
    >
      <h2 style={{ marginBottom: "1rem" }}>CenEMS Telemetry Viewer</h2>

      {error && (
        <div style={{ color: "red", marginBottom: "1rem" }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      <div
        style={{
          display: "flex",
          gap: "1rem",
          marginBottom: "1rem",
          flexWrap: "wrap"
        }}
      >
        <div>
          <label>
            <strong>Building</strong>
            <br />
            <select
              value={selectedBuildingId}
              onChange={(e) => setSelectedBuildingId(e.target.value)}
            >
              <option value="all">All</option>
              {buildings.map((b) => (
                <option key={b.id} value={b.id}>
                  {b.name}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div>
          <label>
            <strong>Device</strong>
            <br />
            <select
              value={selectedDeviceId}
              onChange={(e) => setSelectedDeviceId(e.target.value)}
              disabled={devices.length === 0 && selectedBuildingId !== "all"}
            >
              <option value="all">All</option>
              {devices.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.external_id || d.name || d.id}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div>
          <label>
            <strong>Metric</strong>
            <br />
            <select
              value={selectedMetric}
              onChange={(e) => setSelectedMetric(e.target.value)}
              disabled={metricOptions.length === 0}
            >
              {metricOptions.map((m) => (
                <option key={m.value} value={m.value}>
                  {m.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div>
          <label>
            <strong>Mode</strong>
            <br />
            <select value={metricMode} onChange={(e) => setMetricMode(e.target.value)}>
              <option value="raw">raw</option>
              <option value="delta">delta</option>
            </select>
          </label>
        </div>

        <div>
          <label>
            <strong>Time range</strong>
            <br />
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center" }}>
              {allTime ? (
                <span style={{ padding: "4px 8px", color: "#666" }}>All time</span>
              ) : (
                <>
                  <input
                    type="date"
                    value={dateStart}
                    onChange={(e) => onDateChange("start", e.target.value)}
                    style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
                  />
                  <span style={{ color: "#666" }}>–</span>
                  <input
                    type="date"
                    value={dateEnd}
                    onChange={(e) => onDateChange("end", e.target.value)}
                    style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
                  />
                </>
              )}
              <div style={{ display: "flex", gap: "0.25rem" }}>
                <button
                  type="button"
                  onClick={() => applyPreset("7d")}
                  style={{
                    padding: "4px 8px",
                    fontSize: "0.85rem",
                    background: activePreset === "7d" ? "#e0f2fe" : "transparent",
                    border: "1px solid #ccc",
                    borderRadius: 4,
                    cursor: "pointer"
                  }}
                >
                  Last 7d
                </button>
                <button
                  type="button"
                  onClick={() => applyPreset("30d")}
                  style={{
                    padding: "4px 8px",
                    fontSize: "0.85rem",
                    background: activePreset === "30d" ? "#e0f2fe" : "transparent",
                    border: "1px solid #ccc",
                    borderRadius: 4,
                    cursor: "pointer"
                  }}
                >
                  Last 30d
                </button>
                <button
                  type="button"
                  onClick={() => applyPreset("all")}
                  style={{
                    padding: "4px 8px",
                    fontSize: "0.85rem",
                    background: activePreset === "all" ? "#e0f2fe" : "transparent",
                    border: "1px solid #ccc",
                    borderRadius: 4,
                    cursor: "pointer"
                  }}
                >
                  All time
                </button>
              </div>
            </div>
          </label>
        </div>

        <div>
          <label>
            <strong>Scale</strong>
            <br />
            <select
              value={valuePrefix}
              onChange={(e) => setValuePrefix(Number(e.target.value))}
            >
              {PREFIXES.map((p) => (
                <option key={p.value} value={p.value}>
                  {p.label}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      <section style={{ marginBottom: "1.5rem" }}>
        <h3>Latest readings</h3>
        {selectedDeviceId === "all" ? (
          <p style={{ color: "#555" }}>Select a device to view latest readings.</p>
        ) : !selectedMetric ? (
          <p style={{ color: "#555" }}>Select a metric to view latest readings.</p>
        ) : paginatedRecent.length === 0 ? (
          <p style={{ color: "#555" }}>No readings yet for this device and metric.</p>
        ) : (
          <>
            <div
              style={{
                maxHeight: 280,
                overflowY: "auto",
                border: "1px solid #ddd",
                borderRadius: 4
              }}
            >
              <table
                style={{
                  borderCollapse: "collapse",
                  width: "100%",
                  fontSize: "0.9rem",
                  tableLayout: "fixed"
                }}
              >
                <thead style={{ position: "sticky", top: 0, background: "#f5f5f5" }}>
                  <tr>
                    <th align="left" style={{ width: "15%" }}>Metric</th>
                    <th align="left" style={{ width: "22%" }}>Timestamp</th>
                    <th align="right" style={{ width: "12%" }}>Value</th>
                    <th align="left" style={{ width: "8%" }}>Unit</th>
                    <th align="right" style={{ width: "12%", paddingRight: "1rem" }}>Delta</th>
                    <th align="left" style={{ width: "31%", paddingLeft: "1rem" }}>Quality Flags</th>
                  </tr>
                </thead>
                <tbody>
                  {paginatedRecent.map((m, i) => (
                    <tr key={`${m.ts}-${m.metric}-${i}`}>
                      <td>{m.metric}</td>
                      <td>{new Date(m.ts).toLocaleString()}</td>
                      <td align="right">{m.value.toFixed(3)}</td>
                      <td>{m.unit}</td>
                      <td align="right" style={{ paddingRight: "1rem" }}>
                        {m.delta != null ? m.delta.toFixed(3) : "-"}
                      </td>
                      <td style={{ paddingLeft: "1rem" }}>{formatQualityFlags(m)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div style={{ marginTop: "0.5rem", display: "flex", gap: "0.5rem", alignItems: "center" }}>
              <button
                type="button"
                onClick={() => setRecentPage((p) => Math.max(0, p - 1))}
                disabled={!canGoNext}
              >
                Next (newer)
              </button>
              <button
                type="button"
                onClick={() => setRecentPage((p) => Math.min(totalRecentPages - 1, p + 1))}
                disabled={!canGoPrev}
              >
                Previous (older)
              </button>
              <span style={{ fontSize: "0.85rem", color: "#666" }}>
                Page {recentPage + 1} of {totalRecentPages} ({recentRecords.length} records)
              </span>
            </div>
          </>
        )}
      </section>

      <section>
        <h3>Time-series</h3>
        {loading && <p style={{ color: "#555" }}>Loading…</p>}
        {!loading && chartData.length === 0 && chartsByBuilding.length === 0 ? (
          <p style={{ color: "#555" }}>No data to display.</p>
        ) : selectedBuildingId === "all" && chartsByBuilding.length === 1 && chartsByBuilding[0].buildingName === "Total" ? (
          <div style={{ color: "#666", padding: "1rem", background: "#f9fafb", borderRadius: 8, border: "1px solid #e5e7eb" }}>
            <p style={{ margin: 0 }}>Backend needs a restart to show per-building charts. Run:</p>
            <code style={{ display: "inline-block", marginTop: 6, padding: "4px 8px", background: "#fff", borderRadius: 4, fontSize: "0.9em" }}>
              docker-compose up --build
            </code>
            <p style={{ margin: "8px 0 0", fontSize: "0.9em" }}>Then refresh the page.</p>
          </div>
        ) : selectedBuildingId === "all" && chartsByBuilding.length > 0 ? (
          <div style={{ display: "flex", flexDirection: "column", gap: "2rem" }}>
            {chartsByBuilding.map(({ buildingName, data }, idx) => {
              const color = CHART_COLORS[idx % CHART_COLORS.length];
              const dataKey = metricMode === "delta" ? "delta" : "value";
              return (
                <div key={buildingName}>
                  <h4 style={{ marginBottom: "0.5rem", color }}>
                    {buildingName} (sum of all sensors)
                  </h4>
                  <div style={{ width: "100%", height: 280 }}>
                    <ResponsiveContainer>
                      <LineChart data={data} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="tsLabel" />
                        <YAxis label={{ value: yAxisLabel, angle: -90, position: "insideLeft" }} />
                        <Tooltip
                          content={({ active, payload }) =>
                            active && payload?.[0] ? (
                              <div
                                style={{
                                  background: "#fff",
                                  border: "1px solid #ccc",
                                  borderRadius: 4,
                                  padding: "8px 12px",
                                  boxShadow: "0 2px 8px rgba(0,0,0,0.1)"
                                }}
                              >
                                <div style={{ fontWeight: 600, marginBottom: 4 }}>
                                  {payload[0].payload.building}
                                </div>
                                <div>
                                  {metricMode === "delta" ? "Delta" : "Value"}:{" "}
                                  {payload[0].value?.toLocaleString()} {PREFIXES.find((p) => p.value === valuePrefix)?.label || "kWh"}
                                </div>
                                <div style={{ fontSize: "0.85em", color: "#666" }}>
                                  {payload[0].payload.tsLabel}
                                </div>
                              </div>
                            ) : null
                          }
                        />
                        <Legend />
                        <Line
                          type="monotone"
                          dataKey={dataKey}
                          stroke={color}
                          dot={false}
                          name={buildingName}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <LineChart data={chartData} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="tsLabel" />
                <YAxis label={{ value: yAxisLabel, angle: -90, position: "insideLeft" }} />
                <Tooltip />
                <Legend />
                {aggregatedData.length > 0 && chartData.length > 0 && selectedBuildingId !== "all" ? (
                  Object.keys(chartData[0])
                    .filter((k) => !["ts", "tsLabel"].includes(k))
                    .map((key, i) => (
                      <Line
                        key={key}
                        type="monotone"
                        dataKey={key}
                        stroke={["#2563eb", "#16a34a", "#dc2626"][i % 3]}
                        dot={false}
                        name={key}
                      />
                    ))
                ) : (
                  <Line
                    type="monotone"
                    dataKey={chartDataKey}
                    stroke="#2563eb"
                    dot={false}
                    name={metricMode === "delta" ? "Delta" : "Value"}
                  />
                )}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </section>
    </div>
  );
}

export default App;
