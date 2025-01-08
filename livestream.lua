local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
--local count = 0 for _ in pairs(target) do count = count + 1 end print("disco", count)
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https?://livestream%.com/api/accounts/([0-9]+)$"]="account",
    ["^https?://livestream%.com/api/accounts/([0-9]+/events/[0-9]+)$"]="event",
    ["^https?://livestream%.com/api/accounts/([0-9]+/events/[0-9]+/videos/[0-9]+)$"]="video",
    ["^https?://livestream%.com/api/accounts/([0-9]+/events/[0-9]+/statuses/[0-9]+)$"]="status",
    ["^https?://livestream%.com/api/accounts/([0-9]+/events/[0-9]+/images/[0-9]+)$"]="image",
    ["^(https?://cdn%.livestream%.com/.+)$"]="asset",
    ["^(https?://vpe%-cdn%.livestream%.com/.+)$"]="asset",
    ["^(https?://img%.new%.livestream%.com/.+)$"]="asset"
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    new_item_type = found["type"]
    if new_item_type == "event" or new_item_type == "video" or new_item_type == "status" or new_item_type == "image" then
      local account, event = string.match(found["value"], "^([0-9]+)/events/(.+)$")
      newcontext["account"] = account
      if new_item_type == "event" then
        newcontext["event"] = event
      else
        for k, v in pairs({
          ["video"]="videos",
          ["status"]="statuses",
          ["image"]="images"
        }) do
          if new_item_type == k then
            local event, other = string.match(event, "^([0-9]+)/" .. v .. "/([0-9]+)$")
            newcontext["event"] = event
            newcontext[k] = other
          end
        end
      end
    elseif new_item_type == "account" then
      newcontext["account"] = found["value"]
    elseif not new_item_type == "asset" then
      error("Unknown item type.")
    end
    new_item_value = found["value"]
    if new_item_type ~= "asset" then
      new_item_value = string.gsub(new_item_value, "/[a-z]+/", ":")
    end
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if item_type ~= "asset" then
        ids[string.lower(context[item_type])] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      is_new_design = false
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "^https?://[^/]+/events/[0-9]+$") then
    return false
  end

  local skip = false
  for pattern, type_ in pairs({
    ["/accounts/([0-9]+/events/[0-9]+)$"]="event",
    ["/accounts/([0-9]+/events/[0-9]+/videos/[0-9]+)"]="video",
    ["/accounts/([0-9]+/events/[0-9]+/statuses/[0-9]+)"]="status",
    ["/accounts/([0-9]+/events/[0-9]+/images/[0-9]+)"]="image",
    ["^https?://(cdn%.livestream%.com/.+)$"]="asset",
    ["^https?://(vpe%-cdn%.livestream%.com/.+)$"]="asset",
    ["^https?://(img%.new%.livestream%.com/.+)$"]="asset"
  }) do
    match = string.match(url, pattern)
    if match then
      if type_ ~= "asset" then
        match = string.gsub(match, "/[a-z]+/", ":")
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*livestream%.com/")
    and not string.match(url, "^https?://producer%-api%.appspot%.com/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([0-9]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  if allowed(url)
    and status_code < 300 then
    html = read_file(file)
    if string.match(html, "^%s*{") then
      json = cjson.decode(html)
    end
    if (string.match(url, "^https?://api%.new%.livestream%.com/.")
      or string.match(url, "^https?://livestream%.com/api/."))
      and json then
      check(string.match(url, "^(https?://[^%?]+)"))
      local path = string.match(url, "^https?://[^/]+(/.+)")
      if string.match(path, "/api/") then
        path = string.match(path, "/api(/.+)")
      end
      check("https://api.new.livestream.com" .. path)
      check("https://livestream.com/api" .. path)
    end
    if string.match(url, "^https?://livestream%.com/")
      and not string.match(url, "/player%?")
      and not json then
      local canonical = string.match(html, '<link rel="canonical" href="([^"]+)"')
      if not canonical then
        error("No canonical URL found.")
      end
      ids[canonical] = true
      ids[string.lower(canonical)] = true
      check(canonical)
    end
    if string.match(url, "^https?://livestream%.com/accounts/[0-9]+$")
      or string.match(url, "^https?://livestream%.com/.[^/]+$") then
      for _, p in pairs({"following", "followers"}) do
        local newurl = url .. "/" .. p
        if not string.match(newurl, "/accounts/") then
          ids[newurl] = true
        end
        check(newurl)
      end
    end
    if string.match(url, "^https?://livestream%.com/api/accounts/[0-9]+$") then
      check("https://livestream.com/accounts/" .. context["account"])
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/features")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events?newer=9")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events?older=9")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/following?page=0&maxItems=20")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/followers?page=0&maxItems=20")
      check("https://player-api.new.livestream.com/v3/accounts/" .. context["account"] .. "/availability")
      check("https://player-api.new.livestream.com/v3/accounts/" .. context["account"] .. "/advertising")
    end
    if string.match(url, "^https?://livestream%.com/api/accounts/[0-9]+/events/[0-9]+$") then
      check("https://livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"])
      check("https://producer-api.appspot.com/v1/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/lead_capture_form")
      check("https://donations.livestream.com/v2/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/donation_feature_enabled")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/place")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/classification")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/feed.json")
      for width, height in pairs({
        ["640"]="360",
        ["560"]="315",
        ["960"]="540"
      }) do
        for _, enable_info_and_activity in pairs({"true", "false"}) do
          for _, auto_play in pairs({"true", "false"}) do
            for _, mute in pairs({"true", "false"}) do
              for _, default_drawer in pairs({"", "feed"}) do
                check("https://livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/player?width=" .. width .. "&height=" .. height .. "&enableInfoAndActivity=" .. enable_info_and_activity .. "&defaultDrawer=" .. default_drawer .. "&autoPlay=" .. auto_play .. "&mute=" .. mute)
              end
            end
          end
        end
      end
    end
    if string.match(url, "^https?://livestream%.com/api/accounts/[0-9]+/events/[0-9]+/statuses/[0-9]+$") then
      check("https://livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/statuses/" .. context["status"])
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/statuses/" .. context["status"] .. "/comments")
    end
    if string.match(url, "^https?://livestream%.com/api/accounts/[0-9]+/events/[0-9]+/images/[0-9]+$") then
      check("https://livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/images/" .. context["image"])
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/images/" .. context["image"] .. "/comments")
    end
    if string.match(url, "^https?://livestream%.com/api/accounts/[0-9]+/events/[0-9]+/videos/[0-9]+$") then
      check("https://livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/videos/" .. context["video"])
      check("https://player-api.new.livestream.com/v3/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/videos/" .. context["video"] .. "/player_experiments")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/videos/" .. context["video"] .. "/media")
      check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/videos/" .. context["video"] .. "/comments")
      for width, height in pairs({
        ["640"]="360",
        ["560"]="315",
        ["960"]="540"
      }) do
        for _, enable_info in pairs({"true", "false"}) do
          for _, auto_play in pairs({"true", "false"}) do
            for _, mute in pairs({"true", "false"}) do
              for _, default_drawer in pairs({"", "feed"}) do
                check("https://livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/videos/" .. context["video"] .. "/player?width=" .. width .. "&height=" .. height .. "&enableInfo=" .. enable_info .. "&defaultDrawer=" .. default_drawer .. "&autoPlay=" .. auto_play .. "&mute=" .. mute)
              end
            end
          end
        end
      end
      for _, secure in pairs({"true", "false"}) do
        for _, player in pairs({"true", "false"}) do
          check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/videos/" .. context["video"] .. "/media?secure=" .. secure .. "&player=" .. player)
        end
      end
    end
    if string.match(url, "/accounts/[0-9]+/events/[0-9]+/[a-z]+/[0-9]+/comments[^/]*$") then
      local last_date = nil
      for _, data in pairs(json["data"]) do
        last_date = data["created_at"]
      end
      if last_date then
        local year, month, day, hour, min, sec, msec = string.match(last_date, "^([0-9]+)%-([0-9]+)%-([0-9]+)T([0-9]+):([0-9]+):([0-9]+)%.([0-9]+)Z$")
        
        local timestamp = tostring(os.time({year=year, month=month, day=day, hour=hour, min=min, sec=sec})) .. msec
        timestamp = tonumber(timestamp)
        timestamp = timestamp - 1
        check(string.match(url, "^([^%?]+)") .. "?maxItems=100&timestamp=" .. tostring(timestamp))
      elseif json["total"] > 0 and not string.match(url, "[%?&]timestamp=") then
        error("Error processing comments.")
      end
    end
    if string.match(url, "/media%?secure=") then
      if not json["m3u8"] then
        error("No m3u8 found.")
      end
    end
    if string.match(url, "%.m3u8") then
      for line in string.gmatch(html, "([^\n]+)") do
        if not string.match(line, "^#") then
          line = urlparse.absolute(url, line)
          ids[line] = true
          check(line)
        end
      end
    end
    if string.match(url, "/events/[0-9]+/feed%.json") then
      local last_result = 0
      local last_type = nil
      for _, data in pairs(json["data"]) do
        last_result = data["data"]["id"]
        last_type = data["type"]
        local dirname = ({
          ["video"]="videos",
          ["status"]="statuses",
          ["image"]="images"
        })[last_type]
        check("https://livestream.com/api/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/" .. dirname .. "/" .. last_result)
      end
      if last_result ~= 0 then
        check("https://api.new.livestream.com/accounts/" .. context["account"] .. "/events/" .. context["event"] .. "/feed.json?id=" .. tostring(last_result) .. "&type=" .. last_type .. "&newer=-1&older=10")
      end
    end
    if not string.match(url, "/comments") then
      local page = string.match(url, "[%?&]page=([0-9]+)")
      local max_items = string.match(url, "[%?&]max[iI]tems=([0-9]+)")
      if page or max_items then
        if not page or not max_items then
          error("There should be both a page and maxitems parameters.")
        end
        page = tonumber(page)
        max_items = tonumber(max_items)
        local count = get_count(json["data"])
        if count ~= 0 then
          check(increment_param(url, "page", 0, 1))
        end
      end
    end
    if string.match(url, "/events[^/]*$") then
      for _, data in pairs(json["data"]) do
        check("https://livestream.com/api/accounts/" .. context["account"] .. "/events/" .. data["id"])
      end
      if string.match(url, "[%?&]older=[0-9]+") then
        local older = tonumber(string.match(url, "[%?&]older=([0-9]+)"))
        local count = get_count(json["data"])
        if count == older then
          local real_last = 0
          for _, data in pairs(json["data"]) do
            real_last = data["id"]
          end
          if real_last == 0 then
            error("Could not find last ID.")
          end
          check(set_new_params(url, {["id"]=tostring(real_last),["older"]="24"}))
        elseif json["after"] + count ~= json["total"]
          or json["before"] ~= 0 then
          error("Incomplete page.")
        end
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if is_new_design then
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 5
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["livestream-ho0h30sdf854ze45"] = discovered_items,
    ["urls-2xwc1mibb87ii6n3"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


