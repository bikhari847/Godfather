package com.timstreams

import android.content.Context
import com.lagradost.cloudstream3.plugins.CloudstreamPlugin
import com.lagradost.cloudstream3.plugins.Plugin

@CloudstreamPlugin
class TimStreamsPlugin : Plugin() {
    override fun load(context: Context) {
        TimWeb.context = context.applicationContext
        registerMainAPI(TimStreamsProvider())
    }
}
