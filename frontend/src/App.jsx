/**
 * CenEMS Telemetry Viewer: filter by Building/Device, view Energy time-series
 * (with zoom), latest readings, totals; optional bad records overlay.
 */
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

/** API client: all endpoints use GET except ingest (POST). */
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
  async getRecent(deviceId, metric, limit, offset, includeBad = false) {
    if (deviceId === "all") return [];
    const params = new URLSearchParams({
      metric,
      limit: String(limit),
      offset: String(offset),
      exclude_bad: includeBad ? "false" : "true"
    });
    const res = await fetch(`/devices/${deviceId}/recent?${params.toString()}`);
    if (!res.ok) throw new Error("Failed to fetch recent measurements");
    return res.json();
  },
  async getTimeseries(deviceId, metric, start, end, buildingId, includeBad = false) {
    const excludeBad = !includeBad;
    if (buildingId === "all" || deviceId === "all") {
      const params = new URLSearchParams({
        building_id: buildingId || "all",
        device_id: deviceId || "all",
        metric: metric || "energy_kwh_total",
        exclude_bad: excludeBad ? "true" : "false"
      });
      if (start) params.append("start", start.toISOString());
      if (end) params.append("end", end.toISOString());
      const res = await fetch(`/timeseries/aggregated?${params.toString()}`);
      if (!res.ok) throw new Error("Failed to fetch aggregated timeseries");
      return res.json();
    }
    const params = new URLSearchParams({ device_id: deviceId, metric, exclude_bad: excludeBad ? "true" : "false" });
    if (start) params.append("start", start.toISOString());
    if (end) params.append("end", end.toISOString());
    const res = await fetch(`/timeseries?${params.toString()}`);
    if (!res.ok) throw new Error("Failed to fetch timeseries");
    return res.json();
  },
  async getSumDeltas(buildingId, deviceId, metric, start, end) {
    const params = new URLSearchParams({ building_id: buildingId || "all", device_id: deviceId || "all", metric: metric || "energy_kwh_total" });
    if (start) params.append("start", start.toISOString());
    if (end) params.append("end", end.toISOString());
    const res = await fetch(`/timeseries/sum_deltas?${params.toString()}`);
    if (!res.ok) throw new Error("Failed to fetch sum deltas");
    return res.json();
  },
  async getAggregatedBadPoints(start, end) {
    const params = new URLSearchParams({ building_id: "all", metric: "energy_kwh_total" });
    if (start) params.append("start", start.toISOString());
    if (end) params.append("end", end.toISOString());
    const res = await fetch(`/timeseries/aggregated_bad_points?${params.toString()}`);
    if (!res.ok) throw new Error("Failed to fetch bad points");
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

const PREFIXES = [
  { value: 1, label: "kWh" },
  { value: 1e3, label: "MWh" },
  { value: 1e6, label: "GWh" }
];

const RECORDS_PER_PAGE = 5;

function toDateStr(d) {
  return d.toISOString().slice(0, 10);
}

/** Build start of time range in local time (date + time). */
function toStartDateTime(dateStr, timeStr) {
  if (!dateStr) return null;
  const t = (timeStr || "00:00").trim();
  const [hh, mm] = t.split(":").map((s) => parseInt(s, 10) || 0);
  const d = new Date(dateStr + "T00:00:00");
  d.setHours(hh, mm, 0, 0);
  return d;
}

/** Build end of time range in local time (date + time). */
function toEndDateTime(dateStr, timeStr) {
  if (!dateStr) return null;
  const t = (timeStr || "23:59").trim();
  const [hh, mm] = t.split(":").map((s) => parseInt(s, 10) || 0);
  const d = new Date(dateStr + "T00:00:00");
  d.setHours(hh, mm, 59, 999);
  return d;
}

function App() {
  const [buildings, setBuildings] = useState([]);
  const [selectedBuildingId, setSelectedBuildingId] = useState("all");
  const [devices, setDevices] = useState([]);
  const [selectedDeviceId, setSelectedDeviceId] = useState("all");
  const [latest, setLatest] = useState([]);
  const [recentRecords, setRecentRecords] = useState([]);
  const [recentPage, setRecentPage] = useState(0);
  const [selectedMetric, setSelectedMetric] = useState("energy_kwh_total");
  const [metricMode, setMetricMode] = useState("raw");
  const [dateStart, setDateStart] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    return toDateStr(d);
  });
  const [dateEnd, setDateEnd] = useState(() => toDateStr(new Date()));
  const [timeStart, setTimeStart] = useState("00:00");
  const [timeEnd, setTimeEnd] = useState("23:59");
  const [timeseries, setTimeseries] = useState([]);
  const [aggregatedData, setAggregatedData] = useState([]);
  const [sumDeltas, setSumDeltas] = useState(null);
  const [valuePrefix, setValuePrefix] = useState(1);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [badRecordsOption, setBadRecordsOption] = useState("hide");
  const showBadRecords = badRecordsOption === "show";
  const [allTime, setAllTime] = useState(true);
  const [chartZoomStart, setChartZoomStart] = useState(0);
  const [chartZoomEnd, setChartZoomEnd] = useState(0);
  const [aggregatedBadPoints, setAggregatedBadPoints] = useState([]);

  const getTimeRangeDates = () => {
    if (allTime || !dateStart || !dateEnd) return { start: null, end: null };
    return {
      start: toStartDateTime(dateStart, timeStart),
      end: toEndDateTime(dateEnd, timeEnd)
    };
  };

  const onDateChange = (which, value) => {
    if (which === "start") setDateStart(value);
    else setDateEnd(value);
  };

  useEffect(() => {
    api
      .getBuildings()
      .then((data) => {
        const list = Array.isArray(data) ? data : [];
        setBuildings(list);
        if (list.length > 0) {
          setSelectedBuildingId(list[0].id);
        }
      })
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    if (!selectedBuildingId || selectedBuildingId === "all") {
      setDevices([]);
      setSelectedDeviceId("all");
      setLatest([]);
      setRecentRecords([]);
      setTimeseries([]);
      setSelectedMetric("energy_kwh_total");
      return;
    }
    api
      .getDevices(selectedBuildingId)
      .then((data) => {
        const list = Array.isArray(data) ? data : [];
        setDevices(list);
        if (list.length > 0) {
          setSelectedDeviceId(list[0].id);
        } else {
          setSelectedDeviceId("all");
          setLatest([]);
          setRecentRecords([]);
          setTimeseries([]);
          setSelectedMetric("energy_kwh_total");
        }
      })
      .catch((e) => setError(e.message));
  }, [selectedBuildingId]);

  const apiMetric = selectedMetric || "energy_kwh_total";

  const metricOptions = useMemo(
    () => [{ value: "energy_kwh_total", label: "Energy" }],
    []
  );

  useEffect(() => {
    const values = metricOptions.map((o) => o.value);
    if (selectedMetric && metricOptions.length > 0 && !values.includes(selectedMetric)) {
      const next = metricOptions[0].value;
      if (next !== selectedMetric) setSelectedMetric(next);
    }
  }, [metricOptions, selectedMetric]);

  useEffect(() => {
    if (!selectedDeviceId || !selectedMetric) {
      setRecentRecords([]);
      setRecentPage(0);
      return;
    }
    if (selectedDeviceId === "all") return;
    api
      .getRecent(selectedDeviceId, apiMetric, 100, 0, showBadRecords)
      .then((data) => {
        setRecentRecords(data);
        setRecentPage(0);
      })
      .catch(() => setRecentRecords([]));
  }, [selectedDeviceId, selectedMetric, showBadRecords]);

  useEffect(() => {
    if (!selectedMetric) {
      setTimeseries([]);
      setAggregatedData([]);
      setSumDeltas(null);
      return;
    }
    const useAggregated = selectedBuildingId === "all" || selectedDeviceId === "all";
    const { start, end } = getTimeRangeDates();
    if (!useAggregated && selectedDeviceId) {
      setLoading(true);
      api
        .getTimeseries(selectedDeviceId, apiMetric, start, end, null, showBadRecords)
        .then((data) => {
          setTimeseries(data);
          setAggregatedData([]);
        })
        .catch((e) => setError(e.message))
        .finally(() => setLoading(false));
    } else if (useAggregated) {
      setLoading(true);
      api
        .getTimeseries("all", apiMetric, start, end, selectedBuildingId || "all", false)
        .then((data) => {
          setAggregatedData(Array.isArray(data) ? data : []);
          setTimeseries([]);
        })
        .catch((e) => setError(e.message))
        .finally(() => setLoading(false));
      if (selectedBuildingId === "all" && showBadRecords) {
        api.getAggregatedBadPoints(start, end).then((data) => setAggregatedBadPoints(Array.isArray(data) ? data : [])).catch(() => setAggregatedBadPoints([]));
      } else {
        setAggregatedBadPoints([]);
      }
    } else {
      setTimeseries([]);
      setAggregatedData([]);
    }
  }, [selectedDeviceId, selectedMetric, selectedBuildingId, dateStart, dateEnd, timeStart, timeEnd, allTime, showBadRecords]);

  useEffect(() => {
    if (!selectedMetric) {
      setSumDeltas(null);
      return;
    }
    const { start, end } = getTimeRangeDates();
    api
      .getSumDeltas(selectedBuildingId || "all", selectedDeviceId || "all", apiMetric, start, end)
      .then((data) => setSumDeltas(data.sum_delta))
      .catch(() => setSumDeltas(null));
  }, [selectedBuildingId, selectedDeviceId, selectedMetric, dateStart, dateEnd, timeStart, timeEnd, allTime, showBadRecords]);

  const paginatedRecent = useMemo(() => {
    const start = recentPage * RECORDS_PER_PAGE;
    return recentRecords.slice(start, start + RECORDS_PER_PAGE);
  }, [recentRecords, recentPage]);

  const totalRecentPages = Math.max(1, Math.ceil(recentRecords.length / RECORDS_PER_PAGE));
  const canGoNext = recentPage > 0;
  const canGoPrev = recentPage < totalRecentPages - 1;

  const chartData = useMemo(() => {
    if (aggregatedData.length > 0) {
      const key = metricMode === "delta" ? "delta" : "value";
      const byTs = {};
      const allLabels = new Set();
      aggregatedData.forEach((r) => {
        const ts = r.ts;
        const label = r.label || "Total";
        allLabels.add(label);
        if (!byTs[ts]) byTs[ts] = { tsLabel: new Date(ts).toLocaleTimeString(), ts };
        byTs[ts][label] = (r[key] ?? 0) / valuePrefix;
      });
      const sortedLabels = [...allLabels].sort();
      const sortedTs = Object.keys(byTs).sort((a, b) => new Date(a) - new Date(b));
      const badByTs = {};
      aggregatedBadPoints.forEach((p) => {
        const ts = p.ts;
        const label = p.label || "Total";
        if (!badByTs[ts]) badByTs[ts] = {};
        badByTs[ts][label] = (p[key] ?? 0) / valuePrefix;
      });
      return sortedTs.map((ts) => {
        const row = { tsLabel: byTs[ts].tsLabel, ts: byTs[ts].ts };
        sortedLabels.forEach((label) => {
          row[label] = byTs[ts][label] ?? null;
        });
        sortedLabels.forEach((label) => {
          row["bad_" + label] = badByTs[ts]?.[label] ?? null;
        });
        return row;
      });
    }
    const key = metricMode === "delta" ? "delta" : "value";
    return timeseries
      .filter((p) => showBadRecords || !p.is_bad)
      .map((p) => {
        const val = (metricMode === "delta" ? (p.delta ?? 0) : (p.value || 0)) / valuePrefix;
        return {
          ...p,
          tsLabel: new Date(p.ts).toLocaleTimeString(),
          value: (p.value || 0) / valuePrefix,
          delta: (p.delta ?? 0) / valuePrefix,
          mainValue: p.is_bad ? null : val,
          badValue: p.is_bad ? val : null,
          lateValue: p.is_late ? val : null,
          is_late: p.is_late,
          is_bad: p.is_bad
        };
      });
  }, [timeseries, aggregatedData, aggregatedBadPoints, valuePrefix, metricMode, showBadRecords]);

  const CHART_COLORS = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#4f46e5", "#059669"];
  const AGGREGATED_SERIES_KEYS = ["ts", "tsLabel", "mainValue", "badValue", "lateValue", "is_late", "is_bad"];
  const aggregatedChartKeys =
    aggregatedData.length > 0 && chartData.length > 0
      ? Object.keys(chartData[0]).filter((k) => !AGGREGATED_SERIES_KEYS.includes(k) && !k.startsWith("bad_"))
      : [];
  const aggregatedBadOverlayKeys =
    aggregatedData.length > 0 && chartData.length > 0
      ? Object.keys(chartData[0]).filter((k) => k.startsWith("bad_"))
      : [];

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

  useEffect(() => {
    setChartZoomStart(0);
    setChartZoomEnd(chartData.length);
  }, [chartData.length]);

  const visibleChartData = useMemo(
    () => chartData.slice(chartZoomStart, chartZoomEnd),
    [chartData, chartZoomStart, chartZoomEnd]
  );

  const handleChartZoomIn = () => {
    const len = chartZoomEnd - chartZoomStart;
    if (len <= 1) return;
    const step = Math.max(1, Math.floor(len * 0.1));
    setChartZoomStart((s) => s + step);
    setChartZoomEnd((e) => e - step);
  };
  const handleChartZoomOut = () => {
    const len = chartData.length;
    if (len === 0) return;
    const currentLen = chartZoomEnd - chartZoomStart;
    const step = Math.max(1, Math.floor(currentLen * 0.1));
    setChartZoomStart((s) => Math.max(0, s - step));
    setChartZoomEnd((e) => Math.min(len, e + step));
  };
  const handleChartZoomReset = () => {
    setChartZoomStart(0);
    setChartZoomEnd(chartData.length);
  };

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
        maxWidth: "1200px",
        margin: "0 auto"
      }}
    >
      <h2 style={{ marginBottom: "0.5rem" }}>CenEMS Telemetry Viewer</h2>
      <p style={{ marginBottom: "1rem", fontSize: "0.9rem", color: "#555", maxWidth: "720px" }}>
        Select <strong>Building</strong> {'->'} <strong>All</strong> to show total energy distribution across all buildings. 
        <br />Select <strong>Device</strong> {'->'} <strong>All</strong> to show total energy consumption from all sensors. 
      </p>

      {error && (
        <div style={{ color: "red", marginBottom: "1rem" }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      <div style={{ display: "flex", gap: "2rem", alignItems: "flex-start", flexWrap: "wrap" }}>
        <div style={{ flex: "0 0 260px", minWidth: 200 }}>
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "1rem"
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
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center" }}>
              {allTime ? (
                <span style={{ padding: "4px 8px", color: "#666" }}>Calendar</span>
              ) : (
                <>
                  <input
                    type="date"
                    value={dateStart}
                    onChange={(e) => onDateChange("start", e.target.value)}
                    style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
                  />
                  <input
                    type="time"
                    value={timeStart}
                    onChange={(e) => setTimeStart(e.target.value)}
                    style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
                  />
                  <span style={{ color: "#666" }}>–</span>
                  <input
                    type="date"
                    value={dateEnd}
                    onChange={(e) => onDateChange("end", e.target.value)}
                    style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
                  />
                  <input
                    type="time"
                    value={timeEnd}
                    onChange={(e) => setTimeEnd(e.target.value)}
                    style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
                  />
                </>
              )}
              <button
                type="button"
                onClick={() => setAllTime((a) => !a)}
                style={{
                  padding: "4px 8px",
                  fontSize: "0.85rem",
                  background: allTime ? "#e0f2fe" : "transparent",
                  border: "1px solid #ccc",
                  borderRadius: 4,
                  cursor: "pointer"
                }}
              >
                All time
              </button>
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

        <div>
          <label>
            <strong>Total</strong>
            <br />
            <span style={{ padding: "4px 8px", display: "inline-block", minWidth: 120 }}>
              {sumDeltas != null
                ? `${(sumDeltas / valuePrefix).toLocaleString(undefined, { maximumFractionDigits: 2 })} ${PREFIXES.find((p) => p.value === valuePrefix)?.label || "kWh"}`
                : "—"}
            </span>
          </label>
        </div>

        <div>
          <label>
            <strong>Bad records</strong>
            <p style={{ marginBottom: "1rem", fontSize: "0.9rem", color: "#555", maxWidth: "720px" }}>
              <strong>Bad records</strong> (e.g. unknown units) can be shown or hidden in the chart and table.
            </p>
            <select
              value={badRecordsOption}
              onChange={(e) => setBadRecordsOption(e.target.value)}
              style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ccc" }}
            >
              <option value="show">Show</option>
              <option value="hide">Hide</option>
            </select>
          </label>
        </div>
          </div>
        </div>

        <div style={{ flex: "1", minWidth: 320 }}>
      <section>
        <h3>Time-series</h3>
        {loading && <p style={{ color: "#555" }}>Loading…</p>}
        {!loading && chartData.length === 0 ? (
          <p style={{ color: "#555" }}>No data to display.</p>
        ) : selectedBuildingId === "all" && chartsByBuilding.length === 1 && chartsByBuilding[0].buildingName === "Total" ? (
          <div style={{ color: "#666", padding: "1rem", background: "#f9fafb", borderRadius: 8, border: "1px solid #e5e7eb" }}>
            <p style={{ margin: 0 }}>Backend needs a restart to show per-building charts. Run:</p>
            <code style={{ display: "inline-block", marginTop: 6, padding: "4px 8px", background: "#fff", borderRadius: 4, fontSize: "0.9em" }}>
              docker-compose up --build
            </code>
            <p style={{ margin: "8px 0 0", fontSize: "0.9em" }}>Then refresh the page.</p>
          </div>
        ) : (
          <>
            <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginBottom: "0.5rem", flexWrap: "wrap" }}>
              <span style={{ fontSize: "0.9rem", color: "#555" }}>Chart zoom:</span>
              <button
                type="button"
                onClick={handleChartZoomIn}
                disabled={chartData.length <= 1 || chartZoomEnd - chartZoomStart <= 1}
                style={{ padding: "4px 10px", borderRadius: 4, border: "1px solid #ccc", background: "#f5f5f5", cursor: "pointer" }}
              >
                Zoom in
              </button>
              <button
                type="button"
                onClick={handleChartZoomOut}
                disabled={chartData.length === 0 || (chartZoomStart === 0 && chartZoomEnd === chartData.length)}
                style={{ padding: "4px 10px", borderRadius: 4, border: "1px solid #ccc", background: "#f5f5f5", cursor: "pointer" }}
              >
                Zoom out
              </button>
              <button
                type="button"
                onClick={handleChartZoomReset}
                disabled={chartData.length === 0 || (chartZoomStart === 0 && chartZoomEnd === chartData.length)}
                style={{ padding: "4px 10px", borderRadius: 4, border: "1px solid #ccc", background: "#f5f5f5", cursor: "pointer" }}
              >
                Reset
              </button>
              {chartData.length > 0 && (
                <span style={{ fontSize: "0.85rem", color: "#666" }}>
                  Showing {chartZoomEnd - chartZoomStart} of {chartData.length} points
                </span>
              )}
            </div>
            <div style={{ width: "100%", height: 400 }}>
            <ResponsiveContainer>
              <LineChart data={visibleChartData} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="tsLabel" />
                <YAxis label={{ value: yAxisLabel, angle: -90, position: "insideLeft" }} />
                <Tooltip
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const p = payload[0].payload;
                    const shown = payload.filter((entry) => entry.value != null && entry.dataKey !== "lateValue");
                    const getColor = (entry) =>
                      entry.dataKey?.startsWith("bad_") ? "#9ca3af" : (entry.color ?? (aggregatedChartKeys.length ? CHART_COLORS[aggregatedChartKeys.indexOf(entry.dataKey) % CHART_COLORS.length] : "#333"));
                    return (
                      <div style={{ background: "#fff", border: "1px solid #ccc", borderRadius: 4, padding: "8px 12px", boxShadow: "0 2px 8px rgba(0,0,0,0.1)" }}>
                        <div style={{ fontSize: "0.85em", color: "#666" }}>{p.tsLabel}</div>
                        {p.id != null && <div style={{ fontSize: "0.85em", color: "#666" }}>id: {p.id}</div>}
                        {p.is_late && <div style={{ fontWeight: 600, color: "#dc2626" }}>Late</div>}
                        {shown.map((entry) => (
                          <div key={entry.dataKey} style={{ color: getColor(entry) }}>
                            {entry.name}: {Number(entry.value).toLocaleString()}
                          </div>
                        ))}
                      </div>
                    );
                  }}
                />
                <Legend />
                {aggregatedData.length > 0 && chartData.length > 0 ? (
                  <>
                    {aggregatedChartKeys.map((key, i) => (
                      <Line
                        key={key}
                        type="monotone"
                        dataKey={key}
                        stroke={CHART_COLORS[i % CHART_COLORS.length]}
                        dot={false}
                        connectNulls
                        name={key}
                      />
                    ))}
                    {aggregatedBadOverlayKeys.map((key) => (
                      <Line
                        key={key}
                        type="monotone"
                        dataKey={key}
                        stroke="none"
                        dot={{ r: 4, fill: "#9ca3af" }}
                        connectNulls
                        name={"Bad: " + key.replace("bad_", "")}
                        legendType="none"
                        isAnimationActive={false}
                      />
                    ))}
                  </>
                ) : (
                  <>
                    <Line
                      type="monotone"
                      dataKey={chartData.length > 0 && "mainValue" in chartData[0] ? "mainValue" : chartDataKey}
                      stroke="#2563eb"
                      dot={false}
                      connectNulls
                      name={metricMode === "delta" ? "Delta" : "Value"}
                    />
                    {showBadRecords && chartData.some((d) => d.badValue != null) && (
                      <Line
                        type="monotone"
                        dataKey="badValue"
                        stroke="none"
                        dot={{ r: 4, fill: "#9ca3af" }}
                        connectNulls
                        name="Bad"
                        isAnimationActive={false}
                      />
                    )}
                    {chartData.some((d) => d.lateValue != null) && (
                      <Line
                        type="monotone"
                        dataKey="lateValue"
                        stroke="none"
                        dot={{ r: 4, fill: "#dc2626" }}
                        connectNulls
                        name="Late"
                        isAnimationActive={false}
                      />
                    )}
                  </>
                )}
              </LineChart>
            </ResponsiveContainer>
          </div>
          </>
        )}
      </section>
        </div>
      </div>

      <section style={{ marginTop: "1.5rem", width: "100%" }}>
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
                    <th align="left" style={{ width: "12%" }}>id</th>
                    <th align="left" style={{ width: "22%" }}>Timestamp</th>
                    <th align="right" style={{ width: "12%" }}>Value</th>
                    <th align="left" style={{ width: "8%" }}>Unit</th>
                    <th align="right" style={{ width: "12%", paddingRight: "1rem" }}>Delta</th>
                    <th align="left" style={{ width: "34%", paddingLeft: "1rem" }}>Quality Flags</th>
                  </tr>
                </thead>
                <tbody>
                  {paginatedRecent.map((m, i) => (
                    <tr key={m.raw_event_id != null ? m.raw_event_id : (m.id != null ? m.id : `${m.ts}-${m.metric}-${i}`)}>
                      <td>{m.id != null ? m.id : "—"}</td>
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
    </div>
  );
}

export default App;
