use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) struct Quote {
    pub text: &'static str,
    pub speaker: &'static str,
    pub source: &'static str,
}

const QUOTES: &[Quote] = &[
    Quote {
        text: "I'm gonna make him an offer he can't refuse.",
        speaker: "Don Vito Corleone",
        source: "The Godfather",
    },
    Quote {
        text: "Leave the gun. Take the cannoli.",
        speaker: "Peter Clemenza",
        source: "The Godfather",
    },
    Quote {
        text: "It's not personal, Sonny. It's strictly business.",
        speaker: "Michael Corleone",
        source: "The Godfather",
    },
    Quote {
        text: "Keep your friends close, but your enemies closer.",
        speaker: "Michael Corleone",
        source: "The Godfather Part II",
    },
    Quote {
        text: "A man who doesn't spend time with his family can never be a real man.",
        speaker: "Don Vito Corleone",
        source: "The Godfather",
    },
    Quote {
        text: "Never tell anyone outside the family what you're thinking again.",
        speaker: "Don Vito Corleone",
        source: "The Godfather",
    },
    Quote {
        text: "Some day, and that day may never come, I'll call upon you to do a service for me.",
        speaker: "Don Vito Corleone",
        source: "The Godfather",
    },
    Quote {
        text: "Revenge is a dish that tastes best when served cold.",
        speaker: "Don Vito Corleone",
        source: "The Godfather",
    },
    Quote {
        text: "Funny how? I mean, funny like I'm a clown? I amuse you?",
        speaker: "Tommy DeVito",
        source: "Goodfellas",
    },
    Quote {
        text: "As far back as I can remember, I always wanted to be a gangster.",
        speaker: "Henry Hill",
        source: "Goodfellas",
    },
    Quote {
        text: "Never rat on your friends, and always keep your mouth shut.",
        speaker: "Jimmy Conway",
        source: "Goodfellas",
    },
    Quote {
        text: "Say hello to my little friend!",
        speaker: "Tony Montana",
        source: "Scarface",
    },
    Quote {
        text: "All I have in this world is my balls and my word, and I don't break them for no one.",
        speaker: "Tony Montana",
        source: "Scarface",
    },
    Quote {
        text: "The world is yours.",
        speaker: "Tony Montana",
        source: "Scarface",
    },
    Quote {
        text: "Those who want respect, give respect.",
        speaker: "Tony Soprano",
        source: "The Sopranos",
    },
    Quote {
        text: "A wrong decision is better than indecision.",
        speaker: "Tony Soprano",
        source: "The Sopranos",
    },
    Quote {
        text: "I don't want to be a product of my environment. I want my environment to be a product of me.",
        speaker: "Frank Costello",
        source: "The Departed",
    },
    Quote {
        text: "When you love someone, you've gotta trust them. There's no other way.",
        speaker: "Sam Rothstein",
        source: "Casino",
    },
    Quote {
        text: "You can get further with a kind word and a gun than you can with just a kind word.",
        speaker: "Al Capone",
        source: "The Untouchables",
    },
];

pub(crate) fn random() -> &'static Quote {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as usize)
        .unwrap_or(0);
    let idx = nanos % QUOTES.len();
    &QUOTES[idx]
}
