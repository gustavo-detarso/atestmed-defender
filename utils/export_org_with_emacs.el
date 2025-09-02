;;; export_org_with_emacs.el --- Exporta Org -> PDF em batch para o ATESTMED (verbose)
(require 'cl-lib)

(defun atestmed/tail-file (file &optional n)
  "Imprime as últimas N linhas de FILE (padrão 120)."
  (let ((n (or n 120)))
    (when (and file (file-exists-p file))
      (with-temp-buffer
        (insert-file-contents file)
        (let* ((lines (split-string (buffer-string) "\n"))
               (len (length lines))
               (start (max 0 (- len n))))
          (message "-----[ tail %s ]-----\n%s\n-----[ end tail ]-----"
                   file (mapconcat #'identity (cl-subseq lines start) "\n")))))))

(defun atestmed/export-org-to-pdf (org-file)
  "Exporta ORG-FILE para PDF usando ox-latex (verboso e robusto p/ figuras)."
  (require 'org) (require 'ox) (require 'ox-latex)
  (setq debug-on-error t
      org-latex-remove-logfiles nil
        message-log-max t
        org-export-with-toc nil
        org-export-use-babel nil
        org-latex-prefer-user-labels t)

  (let* ((org-file (expand-file-name org-file))
         (org-dir  (file-name-directory org-file))              ;; .../kpi/orgs/<escopo>/
         (kpi-dir  (expand-file-name "../.." org-dir))          ;; .../kpi/
         (orgs-root (expand-file-name "orgs" kpi-dir))
         (escopo   (file-relative-name org-dir orgs-root))      ;; "top10" ou "individual/<safe>"
         (imgs-dir (expand-file-name (concat "imgs/" escopo "/") kpi-dir)) ;; ABS
         (graphicspath (format "\\graphicspath{\\detokenize{%s}\\detokenize{%s}{./}{../}{../../}}\n"
                               (subst-char-in-string ?\\ ?/ imgs-dir)
                               (subst-char-in-string ?\\ ?/ kpi-dir)))
         (pdf-out   (concat (file-name-sans-extension org-file) ".pdf"))
         (log-out   (concat (file-name-sans-extension org-file) ".log"))
         (org-latex-compiler "xelatex")
         (org-latex-pdf-process
          (list
           (format "sh -xc '%s -interaction=nonstopmode -halt-on-error -file-line-error -output-directory=%%o %%f'"
                   org-latex-compiler)
           (format "sh -xc '%s -interaction=nonstopmode -halt-on-error -file-line-error -output-directory=%%o %%f'"
                   org-latex-compiler)))))

    ;; pacotes & preâmbulo
    (dolist (pkg '("graphicx" "float" "placeins" "longtable" "array" "morefloats"))
      (add-to-list 'org-latex-packages-alist (list "" pkg)))
    (setq org-latex-preamble
          (concat graphicspath
                  "\\floatplacement{figure}{H}\n"
                  "\\usepackage{placeins}\n"
                  "\\usepackage{morefloats}\n"
                  "\\extrafloats{500}\n"
                  "\\setcounter{topnumber}{20}\n"
                  "\\setcounter{bottomnumber}{20}\n"
                  "\\setcounter{totalnumber}{50}\n"
                  "\\renewcommand{\\topfraction}{0.95}\n"
                  "\\renewcommand{\\bottomfraction}{0.95}\n"
                  "\\renewcommand{\\textfraction}{0.05}\n"
                  "\\renewcommand{\\floatpagefraction}{0.85}\n"))

    ;; logs informativos
    (message "[Emacs] org-file : %s" org-file)
    (message "[Emacs] org-dir  : %s" org-dir)
    (message "[Emacs] kpi-dir  : %s" kpi-dir)
    (message "[Emacs] escopo   : %s" escopo)
    (message "[Emacs] imgs-dir : %s" imgs-dir)
    (message "[Emacs] graphicspath: %s" graphicspath)
    (message "[Emacs] latex cmd: %s" (car org-latex-pdf-process))

    (let ((default-directory org-dir)
          (process-connection-type nil)
          (process-environment
           (append
            (list
             (concat "TEXINPUTS=" (subst-char-in-string ?\\ ?/ imgs-dir) ":"
                     (subst-char-in-string ?\\ ?/ kpi-dir) ":")
             "max_print_line=1000")
            process-environment)))
      (condition-case err
          (progn
            (find-file org-file)
            (message "[Emacs] Exportando -> PDF ...")
            (org-latex-export-to-pdf)
            (if (file-exists-p pdf-out)
                (message "[Emacs] OK: %s" pdf-out)
              (progn
                (message "[Emacs] PDF não gerado, exibindo tail do log")
                (atestmed/tail-file log-out 200)
                (error "Falha no LaTeX/PDF"))))
        (error
         (message "[Emacs][ERRO] %s" (error-message-string err))
         (when (file-exists-p log-out)
           (atestmed/tail-file log-out 200))
         (kill-emacs 1))))))

;; Modo batch: usa o 1º argumento como arquivo .org
(when noninteractive
  (let ((file (car command-line-args-left)))
    (unless (and file (file-exists-p file))
      (error "Passe o caminho do .org como argumento."))
    (atestmed/export-org-to-pdf file)))
