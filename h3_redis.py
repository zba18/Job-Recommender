import h3
import redis

class H3RedisWorker:
    # This object uses a redis to store IDs twice. One for low res (for rec_feed 5km distance), and one for hi res (for search query)
    def add_job(self, id_, coords):
        hash_ = h3.geo_to_h3(*coords, self.resolution)
        self.r.append(hash_, f'{id_} ')# add a space after to separate atomically
        hash_hi_res = h3.geo_to_h3(*coords, self.hi_res)
        self.r.append(hash_hi_res, f'{id_} ')# add a space after to separate atomically
        return True

    def find_jobs(self, coords, radius = None):
        #const radius = Math.floor(searchRadiusKm / (h3.edgeLength(res, h3.UNITS.km) * 2));   edgelengh is 1.221 km for res 7 
        #return h3.kRing(origin, radius); https://observablehq.com/@nrabinowitz/h3-radius-lookup?collection=@nrabinowitz/h3-tutorial

        if radius: # high res.
            hex_radius = int(radius/ (2*0.174375668))
            hash_ = h3.geo_to_h3(*coords, self.hi_res)

        else: # no radius supplied; approx 5km for rec_feed 
            hex_radius = 3 # int(radius/ (2*1.220629759))        
            hash_ = h3.geo_to_h3(*coords, self.resolution)
        
        str_list = self.r.mget( h3.k_ring(hash_, hex_radius ) ) #redis always returns bytes 
        ids = b''.join( (x for x in str_list if x ) ).split(b' ') # join if not None, then split by space
        ids = [int(x) for x in ids if x!= b'']
        return ids

    def edit_job_loc(self, id_, old_coords, new_coords):
        '''
        redis transaction that takes the old_coords hash, cuts out the id from that string, then puts it into the new_coords key
        !warning! will fail to remove if old_coords is not the right value, currently doesn't verify this.
        '''
        old_hash = h3.geo_to_h3(*old_coords, self.resolution)
        new_hash = h3.geo_to_h3(*new_coords, self.resolution)
        cmd_response = self.edit_function([id_, old_hash, new_hash])

        old_hash_hi_res = h3.geo_to_h3(*old_coords, self.hi_res)
        new_hash_hi_res = h3.geo_to_h3(*new_coords, self.hi_res)
        cmd_response = self.edit_function([id_, old_hash_hi_res, new_hash_hi_res])
        
        return True

    def del_job_loc(self, id_, old_coords):
        '''
        redis transaction that takes the old_coords hash, cuts out the id from that string
        !warning! will fail to remove if old_coords is not the right value, currently doesn't verify this.
        '''
        old_hash = h3.geo_to_h3(*old_coords, self.resolution)
        cmd_response = self.del_function([id_, old_hash])
        
        old_hash_hi_res = h3.geo_to_h3(*old_coords, self.hi_res)
        cmd_response = self.del_function([id_, old_hash_hi_res])
        
        return f"{id_} no longer in {old_hash} and {old_hash_hi_res}."
    
    
    def load_edit_script(self):
        # get old hash
        # split by ' '
        # look for id 
        # remove that element
        # rebuild string

        script = """ 
        --KEYS: [id, old hash, new hash]
        --SEARCHES AND DELETES OLD ID FROM old hash, THEN APPENDS IT INTO new hash
        local deletion_str = KEYS[1] .. ' '
        local old_hash_str = redis.call('get', KEYS[2])

        local replacement = string.gsub( old_hash_str, "%w+ ", function (s)
        if s == deletion_str then return '' else return s end
        end )

        local set_resp = redis.call('set', KEYS[2], replacement )
        local append_resp = redis.call('append', KEYS[3], deletion_str)

        return append_resp
        """
        
        self.edit_function = self.r.register_script(script) ## keys: [id, old hash, new hash]

    def load_del_script(self):
        # get old hash
        # split by ' '
        # look for id 
        # remove that element
        # rebuild string

        script = """ 
        --KEYS: [id, old hash]
        --SEARCHES AND DELETES OLD ID FROM old hash,
        local deletion_str = KEYS[1] .. ' '
        local old_hash_str = redis.call('get', KEYS[2])

        local replacement = string.gsub( old_hash_str, "%w+ ", function (s)
        if s == deletion_str then return '' else return s end
        end )

        local set_resp = redis.call('set', KEYS[2], replacement )
        return set_resp
        """
        
        self.del_function = self.r.register_script(script) ## keys: [id, old hash, new hash]
        
    def __init__(self, redis_connection_object, h3_resolution = 7, high_resolution = 9):
        self.r = redis_connection_object
        if h3_resolution != 7 or high_resolution != 9:
            raise NotImplementedError # Findjob is hardcoded with distances for res 7 and 9, tweak those before changing this.
        self.resolution = h3_resolution
        self.hi_res = high_resolution
        self.load_edit_script()
        self.load_del_script()

