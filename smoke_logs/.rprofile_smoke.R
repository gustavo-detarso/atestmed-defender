options(error=function(e){
  cat("\n--- TRACEBACK ---\n", file=stderr())
  traceback(2)
  q(status=10, save="no")
})
