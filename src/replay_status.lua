local utils = require "mp.utils"
local pending = {}
local console_ready = false

local function log_to_console(message)
    mp.commandv("script-message-to", "console", "log", utils.format_json({
        log_id = "commands>",
        text = message,
    }))
end

mp.register_script_message("status", function(message)
    if console_ready then
        log_to_console(message)
    else
        pending[#pending + 1] = message
    end
end)

mp.observe_property("user-data/mpv/console/open", "bool", function(_, is_open)
    if not is_open then
        return
    end
    console_ready = true
    for _, message in ipairs(pending) do
        log_to_console(message)
    end
    pending = {}
end)
