# graphs_atestmed.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gráficos do ATESTMED (matplotlib puro; sem seaborn).
Cada função salva UMA figura por arquivo.
"""

import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def save_bar(values, labels, title, out_path, ylabel="Valor"):
    """Barra simples: 1 figura por arquivo."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.figure()
    x = np.arange(len(values))
    plt.bar(x, values)
    plt.xticks(x, labels, rotation=45, ha="right")
    plt.title(title)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

def save_heatmap(matrix, row_labels, col_labels, title, out_path,
                 y_fontsize=None, x_fontsize=None):
    """Heatmap auto-ajustável: 1 figura por arquivo."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    n_rows = max(1, len(row_labels))
    n_cols = max(1, len(col_labels))
    if y_fontsize is None:
        y_fontsize = max(6, min(10, 14 - 0.12 * n_rows))
    if x_fontsize is None:
        x_fontsize = max(6, min(10, 12 - 0.08 * n_cols))
    fig_w = max(6.0, min(16.0, 0.60 * n_cols + 2.0))
    fig_h = max(4.0, min(24.0, 0.35 * n_rows + 2.0))

    plt.figure(figsize=(fig_w, fig_h))
    mat = np.array(matrix, dtype=float) if matrix else np.zeros((1, n_cols))
    im = plt.imshow(mat, aspect="auto")
    plt.xticks(np.arange(n_cols), col_labels, rotation=0, fontsize=x_fontsize)
    plt.yticks(np.arange(n_rows), row_labels, fontsize=y_fontsize)
    plt.title(title)
    cbar = plt.colorbar(im)
    cbar.ax.tick_params(labelsize=max(6, int(min(x_fontsize, y_fontsize) - 1)))
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

def save_lorenz_impact(values, out_path,
                       title: str = "Curva de Lorenz — Impacto entre elegíveis"):
    """
    Lorenz + Gini. Retorna gini (float) ou None se não gera.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    vals = [float(v) for v in values if v is not None and float(v) >= 0.0]
    if not vals or sum(vals) <= 0:
        return None

    v = np.sort(np.array(vals, dtype=float))
    n = v.size
    x = np.arange(n + 1) / n
    y = np.concatenate([[0.0], np.cumsum(v) / v.sum()])
    try:
        area = np.trapezoid(y, x)  # numpy >= 2.0
    except Exception:
        area = np.trapz(y, x)
    gini = max(0.0, min(1.0, 1.0 - 2.0 * area))

    plt.figure()
    plt.plot(x, y, marker="o", linewidth=2, label=f"Lorenz (Gini={gini:.3f})")
    plt.plot([0, 1], [0, 1], linestyle="--", label="Igualdade perfeita")
    plt.title(title)
    plt.xlabel("Proporção acumulada de peritos")
    plt.ylabel("Proporção acumulada do impacto")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()
    return float(gini)

