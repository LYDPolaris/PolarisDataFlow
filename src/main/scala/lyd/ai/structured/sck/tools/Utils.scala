package lyd.ai.structured.sck.tools

object Utils {
  def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }
}

