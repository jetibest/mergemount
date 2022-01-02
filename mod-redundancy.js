// this one must keep track of which files/directories are on which nodes
// which nodes are online/offline
// etc
// if a file is written or updated, this processor will hear about it, and check if the correct nodes have it

// whenever client-fuse detects an event, the event is first passed through the mods
// then the mod can configure which node to select etc.
// by default it falls back to the latest file version, to show a merged view
// but that default latest file version must also be put in a separate mod-latest.js
// we also have a mod-log, which logs all events


module.exports = {
    create: function(options)
    {
        return function(req, res, next)
        {
            next();
        };
    }
};
