local WP = minetest.get_worldpath()
local ie = areas.ie
areas.ie = nil -- remove global
if not ie then
	error("insecure environment inaccessible"..
		" - make sure this mod has been added to minetest.conf!")
end

-- Requires library for db access
local _sql = ie.require("lsqlite3")
if sqlite3 then sqlite3 = nil end
local db = _sql.open(WP.."/areas.sqlite") -- connection

-- Create db:exec wrapper for error reporting
local function db_exec(stmt)
	if db:exec(stmt) ~= _sql.OK then
		minetest.log("info", "Sqlite ERROR:  ", db:errmsg())
	end
end

local create_db = [[
CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY,
name VARCHAR(32), pos1 VARCHAR(512), pos2 VARCHAR(512),
owner VARCHAR(32), parent INTEGER, open BOOLEAN);
CREATE TABLE IF NOT EXISTS _s (import BOOLEAN);
]]
db_exec(create_db)

-- Return a specific record by id
function areas:get_record(id)
	local query = ("SELECT * FROM areas WHERE id = %i LIMIT 1;"):format(id)
	for row in db:nrows(query) do
		return row
	end
end

-- Return all area records
function areas:get_records()
	local result = {}
	local query = "SELECT * FROM areas;"
	for row in db:nrows(query) do
		table.insert(result, row)
	end
	return result
end

-- Update single field in a record by id
function areas:update_by_id(id, field, data)
	local stmt = ([[
		UPDATE areas SET %s = '%s' WHERE id = %i
	]]):format(field, data, id)
	db_exec(stmt)
end

-- Update existing or create new record
function areas:save_record(id, data)
	local r = data
	if self:get_record(id) then
		-- Update
		local stmt = ([[
		UPDATE areas
		SET name = '%s',
			pos1 = '%s',
			pos2 = '%s',
			owner = '%s',
			parent = '%s',
			open = '%s'
		WHERE
			id = %i;
		]]):format(r.name, r.pos1, r.pos2, r.owner, r.parent, r.open, id)
		db_exec(stmt)
		return
	end
	-- Insert record
	local stmt = ([[
	INSERT INTO areas VALUES('%i', '%s', '%s', '%s', '%s', '%s', '%s')
	]]):format(id, r.name, r.pos1, r.pos2, r.owner, r.parent, r.open)
	db_exec(stmt)
end

-- Delete a db record
function areas:remove_record(id)
	local stmt = ([[
	DELETE FROM areas WHERE id = %i
	]]):format(id)
	db_exec(stmt)
end

-- Populate the cache
function areas:load_db()
	local db_records = self:get_records()
	self.areas = self.areas or {}
	for _,entry in ipairs(db_records) do
		self.areas[entry.id] = {
			name = entry.name,
			pos1 = minetest.string_to_pos(entry.pos1),
			pos2 = minetest.string_to_pos(entry.pos2),
			owner = entry.owner
		}
		if entry.parent then self.areas[entry.id].parent = entry.parent end
		if entry.open == "true" then self.areas[entry.id].open = entry.open end
	end
	self:populateStore()
end

local function get_setting(column)
	local query = ([[
		SELECT %s FROM _s
	]]):format(column)
	for row in db:nrows(query) do
		return row
	end
end

function areas:player_exists(name)
	return minetest.get_auth_handler().get_auth(name) ~= nil
end

--- Checks an AreaStore ID.
-- Deletes the AreaStore (falling back to the iterative method)
-- and prints an error message if the ID is invalid.
-- @return Whether the ID was valid.
function areas:checkAreaStoreId(sid)
	if not sid then
		minetest.log("error", "AreaStore failed to find an ID for an "
			.."area!  Falling back to iterative area checking.")
		self.store = nil
		self.store_ids = nil
	end
	return sid and true or false
end

-- Populates the AreaStore after loading, if needed.
function areas:populateStore()
	if not rawget(_G, "AreaStore") then
		return
	end
	local store = AreaStore()
	local store_ids = {}
	for id, area in pairs(areas.areas) do
		local sid = store:insert_area(area.pos1,
			area.pos2, tostring(id))
		if not self:checkAreaStoreId(sid) then
			return
		end
		store_ids[id] = sid
	end
	self.store = store
	self.store_ids = store_ids
end

-- Finds the first usable index in a table
-- Eg: {[1]=false,[4]=true} -> 2
local function findFirstUnusedIndex(t)
	local i = 0
	repeat i = i + 1
	until t[i] == nil
	return i
end

--- Add an area.
-- @return The new area's ID.
function areas:add(owner, name, pos1, pos2, parent)
	local id = findFirstUnusedIndex(self.areas)
	local entry = {
		name = name,
		pos1 = pos1,
		pos2 = pos2,
		owner = owner,
		parent = parent
	}
	self.areas[id] = entry
	-- Add to AreaStore
	if self.store then
		local sid = self.store:insert_area(pos1, pos2, tostring(id))
		if self:checkAreaStoreId(sid) then
			self.store_ids[id] = sid
		end
	end
	-- Format record and add to Db
	entry.pos1 = minetest.pos_to_string(entry.pos1)
	entry.pos2 = minetest.pos_to_string(entry.pos2)
	if entry.parent == nil then entry.parent = '' end
	entry.open = ''
	self:save_record(id, entry)
	return id
end

--- Remove an area, and optionally it's children recursively.
-- If the area is deleted non-recursively the children will
-- have the removed area's parent as their new parent.
function areas:remove(id, recurse)
	if recurse then
		-- Recursively find child entries and remove them
		local cids = self:getChildren(id)
		for _, cid in pairs(cids) do
			self:remove(cid, true)
			self:remove_record(cid)
		end
	else
		-- Update parents
		local parent = self.areas[id].parent
		local children = self:getChildren(id)
		for _, cid in pairs(children) do
			-- The subarea parent will be niled out if the
			-- removed area does not have a parent
			self.areas[cid].parent = parent
			parent = parent or ''
			self:update_by_id(cid, "parent", parent)
		end
	end

	-- Remove main entry
	self.areas[id] = nil
	self:remove_record(id)

	-- Remove from AreaStore
	if self.store then
		self.store:remove_area(self.store_ids[id])
		self.store_ids[id] = nil
	end
end

--- Move an area.
function areas:move(id, area, pos1, pos2)
	area.pos1 = pos1
	area.pos2 = pos2
	-- Update db
	self:update_by_id(id, "pos1", minetest.pos_to_string(pos1))
	self:update_by_id(id, "pos2", minetest.pos_to_string(pos2))

	if self.store then
		self.store:remove_area(areas.store_ids[id])
		local sid = self.store:insert_area(pos1, pos2, tostring(id))
		if self:checkAreaStoreId(sid) then
			self.store_ids[id] = sid
		end
	end
end

-- Checks if an area between two points is entirely contained by another area.
-- Positions must be sorted.
function areas:isSubarea(pos1, pos2, id)
	local area = self.areas[id]
	if not area then
		return false
	end
	local ap1, ap2 = area.pos1, area.pos2
	local ap1x, ap1y, ap1z = ap1.x, ap1.y, ap1.z
	local ap2x, ap2y, ap2z = ap2.x, ap2.y, ap2.z
	local p1x, p1y, p1z = pos1.x, pos1.y, pos1.z
	local p2x, p2y, p2z = pos2.x, pos2.y, pos2.z
	if
			(p1x >= ap1x and p1x <= ap2x) and
			(p2x >= ap1x and p2x <= ap2x) and
			(p1y >= ap1y and p1y <= ap2y) and
			(p2y >= ap1y and p2y <= ap2y) and
			(p1z >= ap1z and p1z <= ap2z) and
			(p2z >= ap1z and p2z <= ap2z) then
		return true
	end
end

-- Returns a table (list) of children of an area given it's identifier
function areas:getChildren(id)
	local children = {}
	for cid, area in pairs(self.areas) do
		if area.parent and area.parent == id then
			table.insert(children, cid)
		end
	end
	return children
end

-- Checks if the user has sufficient privileges.
-- If the player is not a administrator it also checks
-- if the area intersects other areas that they do not own.
-- Also checks the size of the area and if the user already
-- has more than max_areas.
function areas:canPlayerAddArea(pos1, pos2, name)
	local privs = minetest.get_player_privs(name)
	if privs.areas then
		return true
	end

	-- Check self protection privilege, if it is enabled,
	-- and if the area is too big.
	if not self.config.self_protection or
			not privs[areas.config.self_protection_privilege] then
		return false, "Self protection is disabled or you do not have"
				.." the necessary privilege."
	end

	local max_size = privs.areas_high_limit and
			self.config.self_protection_max_size_high or
			self.config.self_protection_max_size
	if
			(pos2.x - pos1.x) > max_size.x or
			(pos2.y - pos1.y) > max_size.y or
			(pos2.z - pos1.z) > max_size.z then
		return false, "Area is too big."
	end

	-- Check number of areas the user has and make sure it not above the max
	local count = 0
	for _, area in pairs(self.areas) do
		if area.owner == name then
			count = count + 1
		end
	end
	local max_areas = privs.areas_high_limit and
			self.config.self_protection_max_areas_high or
			self.config.self_protection_max_areas
	if count >= max_areas then
		return false, "You have reached the maximum amount of"
				.." areas that you are allowed to  protect."
	end

	-- Check intersecting areas
	local can, id = self:canInteractInArea(pos1, pos2, name)
	if not can then
		local area = self.areas[id]
		return false, ("The area intersects with %s [%u] (%s).")
				:format(area.name, id, area.owner)
	end

	return true
end

-- Given a id returns a string in the format:
-- "name [id]: owner (x1, y1, z1) (x2, y2, z2) -> children"
function areas:toString(id)
	local area = self.areas[id]
	local message = ("%s [%d]: %s %s %s"):format(
		area.name, id, area.owner,
		minetest.pos_to_string(area.pos1),
		minetest.pos_to_string(area.pos2))

	local children = areas:getChildren(id)
	if #children > 0 then
		message = message.." -> "..table.concat(children, ", ")
	end
	return message
end

-- Re-order areas in table by their identifiers
function areas:sort()
	local sa = {}
	for k, area in pairs(self.areas) do
		if not area.parent then
			table.insert(sa, area)
			local newid = #sa
			for _, subarea in pairs(self.areas) do
				if subarea.parent == k then
					subarea.parent = newid
					table.insert(sa, subarea)
				end
			end
		end
	end
	self.areas = sa
end

-- Checks if a player owns an area or a parent of it
function areas:isAreaOwner(id, name)
	local cur = self.areas[id]
	if cur and minetest.check_player_privs(name, self.adminPrivs) then
		return true
	end
	while cur do
		if cur.owner == name then
			return true
		elseif cur.parent then
			cur = self.areas[cur.parent]
		else
			return false
		end
	end
	return false
end

-- Import data
if get_setting("import") == nil then
	-- This conditional branch runs once the first time
	-- and manages the importation of the records from the
	-- areas.dat file to the areas.sqlite database
	--
	-- Load the areas table from the db
	function areas:load()
		local file, err = io.open(self.config.filename, "r")
		if err then
			self.areas = self.areas or {}
			return err
		end
		self.areas = minetest.deserialize(file:read("*a"))
		if type(self.areas) ~= "table" then
			self.areas = {}
		end
		file:close()
		self:populateStore()
	end
	-- Db import
	function areas:import_db()
		for i,entry in ipairs(self.areas) do
			local r = {
				name = entry.name,
				pos1 = minetest.pos_to_string(entry.pos1),
				pos2 = minetest.pos_to_string(entry.pos2),
				owner = entry.owner,
				parent = entry.parent or '',
				open = entry.open or ''
			}
			self:save_record(i, r)
		end
	end
	local function import_task()
		areas:load()
		areas:import_db()
		db_exec([[INSERT INTO _s VALUES('true');]])
		ie.os.rename(WP.."/areas.dat", WP.."/areas.dat.bak")
	end
	-- Execute import after server has started
	minetest.after(5, import_task)
end
