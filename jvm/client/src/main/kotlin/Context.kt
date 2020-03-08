package org.ballistacompute.client

class Context(endpoint: Endpoint) {
    val client = Client(endpoint.host, endpoint.port)


}


class ContextBuilder {

    var master: String? = null

    fun master(master: String): ContextBuilder {
        this.master = master
        return this
    }

    fun build(): Context {
        return Context(parseMaster(this.master))
    }

    private fun parseMaster(master: String?) : Endpoint {
        val parts = this.master?.split(':')
        if (parts?.size == 2) {
            return Endpoint(parts[0], parts[1].toInt())
        } else {
            throw IllegalStateException()
        }
    }
}

data class Endpoint(val host: String, val port: Int)