
// Firebase init
const firebaseConfig = {
  apiKey: "AIzaSyApUcFwneC85iAajpMYu0hpczwe3iQ0CyA",
  authDomain: "track-ce817.firebaseapp.com",
  projectId: "track-ce817",
  storageBucket: "track-ce817.firebasestorage.app",
  messagingSenderId: "207486826025",
  appId: "1:207486826025:web:a42aeca80955f819064e38"
};
firebase.initializeApp(firebaseConfig);
const db = firebase.firestore();
const auth = firebase.auth();

// Live user counter: 280 hardcoded offset + registered user count in Firestore meta/userCount.
const USER_COUNT_OFFSET = 280;
const userCountRef = db.collection("meta").doc("userCount");
function renderUserCounter(n) {
  const el = document.getElementById("user-counter");
  if (el) el.textContent = (USER_COUNT_OFFSET + (n || 0)) + " REGISTERED USERS";
}
// Real-time listener — updates for everyone whenever the count doc changes.
userCountRef.onSnapshot((snap) => {
  if (snap.exists && typeof snap.data().count === "number") {
    renderUserCounter(snap.data().count);
  }
}, (err) => { console.error("User counter listener failed:", err); });
// Bootstrap: called after auth so we have permission to read the users collection.
async function bootstrapUserCountIfNeeded() {
  try {
    const snap = await userCountRef.get();
    if (!snap.exists || typeof snap.data().count !== "number") {
      const all = await db.collection("users").get();
      const n = all.size;
      await userCountRef.set({ count: n }, { merge: true });
      // onSnapshot will fire and call renderUserCounter automatically.
    }
  } catch(e) { console.error("User count bootstrap failed:", e); }
}
let DOC_REF = null;
let currentUser = null;
let viewingUser = null; // null = viewing own list, otherwise { uid, name, photo }
let myData = null; // backup of own data when viewing others
let userProfile = null; // custom display name and photo
const UI_STATE_KEY = 'screenlist-ui-state-v2';
const CREATOR_ADMIN_EMAIL = 'kingkooom@gmail.com';
const CREATOR_DEFAULT_NAME = 'King Kooom';
let commentsViewState = null;

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function getUserAccountEmail(userLike = null) {
  if (!userLike) return '';
  return normalizeEmail(
    userLike.accountEmailLower ||
    userLike.emailLower ||
    userLike.accountEmail ||
    userLike.email ||
    ''
  );
}

function isCreatorAdmin(userLike = null) {
  const candidateEmail = getUserAccountEmail(userLike);
  if (candidateEmail && candidateEmail === CREATOR_ADMIN_EMAIL) return true;
  if (userLike?.uid && currentUser && userLike.uid === currentUser.uid) {
    return normalizeEmail(currentUser.email) === CREATOR_ADMIN_EMAIL;
  }
  return false;
}

function getDisplayName(userLike = null, fallback = 'Unknown User') {
  return (userLike && (userLike.name || userLike.customName)) || fallback;
}

function renderDisplayNameHTML(userLike = null, fallback = 'Unknown User', extraClass = '') {
  const classes = ['creator-name'];
  if (extraClass) classes.push(extraClass);
  const nameHtml = escHtml(getDisplayName(userLike, fallback));
  if (isCreatorAdmin(userLike)) {
    return `<span class="${classes.join(' ')}">👑 ${nameHtml}</span>`;
  }
  return `<span${extraClass ? ` class="${extraClass}"` : ''}>${nameHtml}</span>`;
}

function shouldExposeInUserSearch(userLike = null) {
  return isCreatorAdmin(userLike) && userLike?.isPublic !== false;
}

function getEmptyListData() {
  return { movies: [], shows: [], anime: [], games: [] };
}

function isShowSection(section) {
  return section === "shows" || section === "anime";
}

function getDefaultTabForSection(section) {
  return section === "movies" ? "planned" : "watching";
}

function getSectionLabel(section, singular = false) {
  if (section === "movies") return singular ? "movie" : "movies";
  if (section === "anime") return singular ? "anime" : "anime";
  if (section === "games") return singular ? "game" : "games";
  return singular ? "show" : "shows";
}

function getSectionIcon(section) {
  if (section === "movies") return "🎬";
  if (section === "anime") return "🌸";
  if (section === "games") return "🎮";
  return "📺";
}

function detectJapaneseScript(value) {
  return /[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]/.test(String(value || ""));
}

function toGenreNameList(item) {
  if (Array.isArray(item?.genreNames)) {
    return item.genreNames.map(name => String(name || '').trim().toLowerCase()).filter(Boolean);
  }
  return String(item?.genre || '')
    .split(',')
    .map(name => name.trim().toLowerCase())
    .filter(Boolean);
}

function toOriginCountryList(item) {
  if (Array.isArray(item?.originCountries)) {
    return item.originCountries.map(code => String(code || '').trim().toUpperCase()).filter(Boolean);
  }
  return String(item?.originCountries || '')
    .split(',')
    .map(code => code.trim().toUpperCase())
    .filter(Boolean);
}

function detectAnimeFromMetadata(item) {
  const genres = toGenreNameList(item);
  const hasAnimationGenre = genres.includes('animation');
  if (!hasAnimationGenre) return false;

  const originalLanguage = String(item?.originalLanguage || '').trim().toLowerCase();
  const originCountries = toOriginCountryList(item);
  const originalTitle = item?.originalTitle || item?.originalName || '';
  const title = item?.title || '';
  const hasJapaneseSignal =
    originalLanguage === 'ja' ||
    originCountries.includes('JP') ||
    detectJapaneseScript(originalTitle) ||
    detectJapaneseScript(title);

  return hasJapaneseSignal;
}

function resolveShowSection(item, fallbackSection = "shows") {
  if (!isShowSection(fallbackSection)) return fallbackSection;
  const explicit = String(item?.librarySection || item?.mediaCategory || '').trim().toLowerCase();
  if (explicit === 'anime') return 'anime';
  if (explicit === 'shows' || explicit === 'show' || explicit === 'tv') return 'shows';
  return detectAnimeFromMetadata(item) ? 'anime' : 'shows';
}

function normalizeListEntry(item, fallbackSection) {
  if (!item || typeof item !== 'object') return null;
  const next = { ...item };
  if (isShowSection(fallbackSection)) {
    const resolvedSection = resolveShowSection(next, fallbackSection);
    next.mediaCategory = resolvedSection;
    next.librarySection = resolvedSection;
    next.isAnime = resolvedSection === 'anime';
  } else {
    next.librarySection = fallbackSection;
  }
  return next;
}

function normalizeListData(source) {
  const normalized = getEmptyListData();
  const input = source && typeof source === 'object' ? source : {};

  ["movies", "games"].forEach(section => {
    const items = Array.isArray(input[section]) ? input[section] : [];
    normalized[section] = items
      .map(item => normalizeListEntry(item, section))
      .filter(Boolean);
  });

  ["shows", "anime"].forEach(section => {
    const items = Array.isArray(input[section]) ? input[section] : [];
    items.forEach(item => {
      const normalizedItem = normalizeListEntry(item, section);
      if (!normalizedItem) return;
      normalized[resolveShowSection(normalizedItem, section)].push(normalizedItem);
    });
  });

  return JSON.parse(JSON.stringify(normalized));
}

async function hydrateShowMetadataFromTmdb(item) {
  if (!item?.tmdbId) return false;
  try {
    const res = await fetchTmdbProxy(`tv/${item.tmdbId}`);
    if (!res.ok) return false;
    const d = await res.json();
    item.genre = (d.genres || []).map(g => g.name).join(', ');
    item.genreNames = (d.genres || []).map(g => g.name).filter(Boolean);
    item.originalTitle = d.original_name || item.originalTitle || '';
    item.originalLanguage = d.original_language || item.originalLanguage || '';
    item.originCountries = Array.isArray(d.origin_country) ? d.origin_country : (item.originCountries || []);
    item.mediaCategory = detectAnimeFromMetadata(item) ? 'anime' : 'shows';
    item.librarySection = item.mediaCategory;
    item.isAnime = item.mediaCategory === 'anime';
    return true;
  } catch (e) {
    console.error('Anime classification refresh failed:', e);
    return false;
  }
}

async function autoSortAnimeBuckets(source, persist = false) {
  const working = cloneListData(source);
  const candidates = [...(working.shows || []), ...(working.anime || [])]
    .filter(item => item?.tmdbId)
    .filter(item =>
      !item.librarySection ||
      !item.mediaCategory ||
      !item.originalLanguage ||
      !Array.isArray(item.originCountries) ||
      !Array.isArray(item.genreNames)
    );

  let enriched = false;
  for (const item of candidates) {
    const changed = await hydrateShowMetadataFromTmdb(item);
    enriched = enriched || changed;
  }

  const normalized = normalizeListData(working);
  if (!enriched && isSameListData(normalized, source)) return normalized;

  if (persist && currentUser && !viewingUser) {
    await writeOwnDataDirect(normalized);
  }

  return normalized;
}

let data = getEmptyListData();
let ownDataCache = null; // durable in-session copy of the signed-in user's own library
let friendViewData = null; // isolated data for a friend's profile; never used for saving your own list
function cloneListData(source) {
  return normalizeListData(source);
}
function getVisibleListData() {
  return viewingUser && friendViewData ? friendViewData : data;
}
function listDataSignature(source) {
  return JSON.stringify(cloneListData(source));
}
function isSameListData(a, b) {
  return !!a && !!b && listDataSignature(a) === listDataSignature(b);
}
function listDataItemCount(source) {
  const d = cloneListData(source);
  return d.movies.length + d.shows.length + d.anime.length + d.games.length;
}
function readOwnLocalBackup(excludeData = null) {
  const keys = [];
  if (currentUser) keys.push("screenlist-own-data-backup-" + currentUser.uid);
  keys.push("watchlist-tracker-data");
  for (const key of keys) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      const parsed = cloneListData(JSON.parse(raw));
      if (listDataItemCount(parsed) === 0) continue;
      if (excludeData && isSameListData(parsed, excludeData)) continue;
      return parsed;
    } catch(e) {}
  }
  return null;
}
async function writeOwnDataDirect(nextData) {
  const safeData = cloneListData(nextData);
  data = cloneListData(safeData);
  ownDataCache = cloneListData(safeData);
  if (currentUser) localStorage.setItem("screenlist-own-data-backup-" + currentUser.uid, JSON.stringify(safeData));
  localStorage.setItem("watchlist-tracker-data", JSON.stringify(safeData));
  if (DOC_REF) {
    await DOC_REF.set({
      shows: JSON.stringify(safeData.shows),
      movies: JSON.stringify(safeData.movies),
      anime: JSON.stringify(safeData.anime),
      games: JSON.stringify(safeData.games),
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  }
  return safeData;
}
async function loadOwnDataFromFirestore() {
  if (!DOC_REF) return ownDataCache ? cloneListData(ownDataCache) : cloneListData(data);
  try {
    const snap = await DOC_REF.get();
    if (!snap.exists) return getEmptyListData();
    const d = snap.data();
    return normalizeListData({
      shows: d.shows ? JSON.parse(d.shows) : [],
      movies: d.movies ? JSON.parse(d.movies) : [],
      anime: d.anime ? JSON.parse(d.anime) : [],
      games: d.games ? JSON.parse(d.games) : []
    });
  } catch(e) {
    console.error("Own library reload failed:", e);
    return ownDataCache ? cloneListData(ownDataCache) : cloneListData(data);
  }
}
let activeSection = "shows";
let activeTab = "watching";
let searchQuery = "";
let openStates = {};
let saveTimeout = null;

// Sort state — session-only (resets on page refresh/leave, never persisted)
let sessionSortState = {};   // { "section:tab": sortKey }
let sessionCustomOrder = {}; // { "section:tab": [id, ...] }
const DEFAULT_SORT = 'recently-modified';
const SORT_OPTIONS = [
  { key: 'recently-modified', label: 'Recently Updated' },
  { key: 'recently-added',    label: 'Recently Added' },
  { key: 'title-az',          label: 'Title A–Z' },
  { key: 'rating-high',       label: 'Highest Rated' },
  { key: 'rating-low',        label: 'Lowest Rated' },
  { key: 'release-newest',    label: 'Newest Release' },
  { key: 'release-oldest',    label: 'Oldest Release' },
  { key: 'custom',            label: 'Custom Order' },
];

function getSortStateKey() { return activeSection + ':' + activeTab; }
function getActiveSortKey() { return sessionSortState[getSortStateKey()] || DEFAULT_SORT; }

function setSortOrder(key) {
  const stateKey = getSortStateKey();
  sessionSortState[stateKey] = key;
  if (key === 'custom' && !sessionCustomOrder[stateKey]) {
    const visibleData = getVisibleListData();
    const items = (visibleData[activeSection] || []).filter(i => i.status === activeTab);
    sessionCustomOrder[stateKey] = applySortOrder(items, DEFAULT_SORT, stateKey).map(i => i.id);
  }
  closeSortDropdown();
  render();
}

function applySortOrder(items, sortKey, stateKey) {
  const arr = [...items];
  switch (sortKey) {
    case 'recently-modified':
      return arr.sort((a, b) =>
        new Date(b.dateModified || b.dateAdded || 0) - new Date(a.dateModified || a.dateAdded || 0));
    case 'recently-added':
      return arr.sort((a, b) =>
        new Date(b.dateAdded || 0) - new Date(a.dateAdded || 0));
    case 'title-az':
      return arr.sort((a, b) => (a.title || '').localeCompare(b.title || ''));
    case 'rating-high':
      return arr.sort((a, b) => (b.rating || 0) - (a.rating || 0));
    case 'rating-low':
      return arr.sort((a, b) => (a.rating || 0) - (b.rating || 0));
    case 'release-newest':
      return arr.sort((a, b) => (parseInt(b.year) || 0) - (parseInt(a.year) || 0));
    case 'release-oldest':
      return arr.sort((a, b) => (parseInt(a.year) || 0) - (parseInt(b.year) || 0));
    case 'custom': {
      const order = (stateKey && sessionCustomOrder[stateKey]) || [];
      if (!order.length) return arr;
      const idx = {};
      order.forEach((id, i) => { idx[id] = i; });
      return arr.sort((a, b) =>
        (idx[a.id] !== undefined ? idx[a.id] : 9999) -
        (idx[b.id] !== undefined ? idx[b.id] : 9999));
    }
    default:
      return arr;
  }
}

function closeSortDropdown() {
  const m = document.getElementById('sort-dropdown-menu');
  if (m) m.remove();
}

function toggleSortDropdown(e) {
  if (e) e.stopPropagation();
  const existing = document.getElementById('sort-dropdown-menu');
  if (existing) { existing.remove(); return; }
  const btn = document.getElementById('sort-dropdown-btn');
  if (!btn) return;
  const activeSortKey = getActiveSortKey();
  const menu = document.createElement('div');
  menu.id = 'sort-dropdown-menu';
  menu.className = 'sort-dropdown-menu';
  SORT_OPTIONS.forEach(opt => {
    const el = document.createElement('button');
    el.className = 'sort-dropdown-item' + (opt.key === activeSortKey ? ' active' : '');
    el.textContent = opt.label;
    el.onclick = () => setSortOrder(opt.key);
    menu.appendChild(el);
  });
  document.body.appendChild(menu);
  const rect = btn.getBoundingClientRect();
  menu.style.top = (rect.bottom + window.scrollY + 4) + 'px';
  const rightOffset = window.innerWidth - rect.right;
  menu.style.right = rightOffset + 'px';
  setTimeout(() => document.addEventListener('click', closeSortDropdown, { once: true }), 0);
}

// Stamp dateModified on an item whenever it is mutated
// Stamp dateModified so "Recently Updated" sort stays accurate after mutations
function touchItem(item) {
  if (item) item.dateModified = new Date().toISOString();
}

// Custom order drag-and-drop
let _dragSrcId = null;
function onCardDragStart(e, id) {
  _dragSrcId = id;
  e.dataTransfer.effectAllowed = 'move';
}
function onCardDragOver(e) {
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  const card = e.currentTarget;
  card.classList.add('drag-over');
}
function onCardDragLeave(e) {
  e.currentTarget.classList.remove('drag-over');
}
function onCardDrop(e, id) {
  e.preventDefault();
  e.currentTarget.classList.remove('drag-over');
  if (!_dragSrcId || _dragSrcId === id) return;
  const stateKey = getSortStateKey();
  let order = sessionCustomOrder[stateKey] ? [...sessionCustomOrder[stateKey]] : [];
  const from = order.indexOf(_dragSrcId);
  const to = order.indexOf(id);
  if (from === -1 || to === -1) return;
  order.splice(from, 1);
  order.splice(to, 0, _dragSrcId);
  sessionCustomOrder[stateKey] = order;
  _dragSrcId = null;
  render();
}
let friends = []; // mutually confirmed friends (UIDs)
let incomingRequests = []; // requests they sent me, awaiting my accept
let outgoingRequests = []; // requests I sent, awaiting their accept
let activeFriendsTab = 'friends';
let mainNavSwitching = false;
let allUsersCache = []; // search result cache for Find People
let peopleSearchTimeout = null;
let usersMap = {}; // uid -> { name, photo } for safe lookups
let friendsDataUnsubscribe = null; // realtime listener for friends/requests
let friendsDataLoadedOnce = false;

function isPreviewMode() {
  return document.body.classList.contains('preview-mode');
}
function getPreviewItemCount() {
  return [...(data.movies||[]), ...(data.shows||[]), ...(data.anime||[]), ...(data.games||[])].filter(i => (i.title || '').trim() !== '').length;
}
function ensurePreviewCap() {
  if (!isPreviewMode()) return true;
  const sectionCount = (data[activeSection] || []).filter(i => (i.title || '').trim() !== '').length;
  if (sectionCount >= 2) {
    showToast("Preview limit reached. You can test up to 2 titles per section before signing in.");
    return false;
  }
  return true;
}

function isDuplicateTitle(title, section, excludeId = null) {
  const normalized = (title || '').trim().toLowerCase();
  if (!normalized) return false;
  return (data[section] || []).some(item =>
    item &&
    item.id !== excludeId &&
    (item.title || '').trim().toLowerCase() === normalized
  );
}

const DEMO_DATA = {
  shows: [
    { id:'d1', title:'Game of Thrones', genre:'Drama, Sci-Fi & Fantasy, Action & Adventure', status:'watching', rating:9, currentEp:3, totalEps:73, imdbId:'tt0944947', cover:'https://image.tmdb.org/t/p/w500/u3bZgnGQ9T01sWNhyveQz0wH0Hl.jpg', episodes:[
      { id:'got-s1e1', season:1, number:1, title:'Winter Is Coming', watched:true, rating:9 },
      { id:'got-s1e2', season:1, number:2, title:'The Kingsroad', watched:true, rating:8 },
      { id:'got-s1e3', season:1, number:3, title:'Lord Snow', watched:true, rating:8 },
      { id:'got-s1e4', season:1, number:4, title:'Cripples, Bastards, and Broken Things', watched:false, rating:0 },
      { id:'got-s1e5', season:1, number:5, title:'The Wolf and the Lion', watched:false, rating:0 }
    ] }
  ],
  anime: [],
  movies: [
    { id:'m1', title:'Spider-Man: Across the Spider-Verse', genre:'Animation, Action, Adventure', status:'planned', rating:9, imdbId:'tt9362722', cover:'https://image.tmdb.org/t/p/w500/8Vt6mWEReuy4Of61Lnj5Xj704m8.jpg' }
  ],
  games: [
    { id:'g1', title:'GTA V', genre:'Action, Open World', status:'watching', rating:9, currentEp:45, totalEps:69, cover:'https://images.igdb.com/igdb/image/upload/t_cover_big/co2lbd.jpg', episodes:[] }
  ]
};

const PREVIEW_COMMUNITY_USERS = [
  {
    uid: 'preview-lena',
    name: 'Lena Knox',
    photo: 'https://ui-avatars.com/api/?name=Lena+Knox&background=1e2028&color=60a5fa',
    stats: '12 tracked · Watching 4',
    findStats: 'Public preview profile · Tap to explore',
    listData: {
      shows: [
        { id: 'pl-s1', title: 'Game of Thrones', genre: 'Drama, Sci-Fi & Fantasy', status: 'watching', rating: 10, currentEp: 6, totalEps: 73, imdbId: 'tt0944947', cover: 'https://image.tmdb.org/t/p/w500/u3bZgnGQ9T01sWNhyveQz0wH0Hl.jpg' },
        { id: 'pl-s2', title: 'Severance', genre: 'Drama, Mystery, Sci-Fi & Fantasy', status: 'planned', rating: 0, totalEps: 19, imdbId: 'tt11280740', cover: 'https://image.tmdb.org/t/p/w500/7WTsnHkbA0FaG6R9twfFde0I9hl.jpg' }
      ],
      movies: [
        { id: 'pl-m1', title: 'Dune: Part Two', genre: 'Science Fiction, Adventure', status: 'watched', rating: 9, imdbId: 'tt15239678', cover: 'https://image.tmdb.org/t/p/w500/1pdfLvkbY9ohJlCjQH2CZjjYVvJ.jpg' }
      ],
      anime: [
        { id: 'pl-a1', title: 'Frieren: Beyond Journey’s End', genre: 'Animation, Drama, Fantasy', status: 'watching', rating: 9, totalEps: 28, imdbId: 'tt22248376', cover: 'https://image.tmdb.org/t/p/w500/dqZENchTd7lp5zht7BdlqM7RBhN.jpg' }
      ],
      games: [
        { id: 'pl-g1', title: 'Hades', genre: 'Roguelike, Action', status: 'watched', rating: 10, cover: 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1r7f.jpg', episodes: [] }
      ]
    }
  },
  {
    uid: 'preview-marcus',
    name: 'Marcus Vale',
    photo: 'https://ui-avatars.com/api/?name=Marcus+Vale&background=2a1f5e&color=f8fafc',
    stats: '9 tracked · Rated 7 this month',
    findStats: 'Preview community member · Tap to explore',
    listData: {
      shows: [
        { id: 'pm-s1', title: 'Andor', genre: 'Drama, Sci-Fi & Fantasy', status: 'watched', rating: 9, totalEps: 12, imdbId: 'tt9253284', cover: 'https://image.tmdb.org/t/p/w500/59SVNwLfoMnZPPB6ukW6dlPxAdI.jpg' }
      ],
      movies: [
        { id: 'pm-m1', title: 'Spider-Man: Across the Spider-Verse', genre: 'Animation, Action, Adventure', status: 'planned', rating: 9, imdbId: 'tt9362722', cover: 'https://image.tmdb.org/t/p/w500/8Vt6mWEReuy4Of61Lnj5Xj704m8.jpg' }
      ],
      anime: [],
      games: [
        { id: 'pm-g1', title: 'Baldur’s Gate 3', genre: 'RPG, Strategy', status: 'watching', rating: 10, cover: 'https://images.igdb.com/igdb/image/upload/t_cover_big/co670h.jpg', episodes: [] }
      ]
    }
  },
  {
    uid: 'preview-yara',
    name: 'Yara Bloom',
    photo: 'https://ui-avatars.com/api/?name=Yara+Bloom&background=111827&color=93c5fd',
    stats: '14 tracked · Loves anime nights',
    findStats: 'Preview community member · Tap to explore',
    listData: {
      shows: [
        { id: 'py-s1', title: 'The Bear', genre: 'Drama, Comedy', status: 'planned', rating: 0, totalEps: 28, imdbId: 'tt14452776', cover: 'https://image.tmdb.org/t/p/w500/sHFlbKS3WLqMnp9tN5J6Lr3q13Q.jpg' }
      ],
      movies: [
        { id: 'py-m1', title: 'Everything Everywhere All at Once', genre: 'Action, Adventure, Science Fiction', status: 'watched', rating: 10, imdbId: 'tt6710474', cover: 'https://image.tmdb.org/t/p/w500/w3LxiVYdWWRvEVdn5RYq6jIqkb1.jpg' }
      ],
      anime: [
        { id: 'py-a1', title: 'Attack on Titan', genre: 'Animation, Action & Adventure, Sci-Fi & Fantasy', status: 'watched', rating: 10, totalEps: 89, imdbId: 'tt2560140', cover: 'https://image.tmdb.org/t/p/w500/hTP1DtLGFamjfu8WqjnuQdP1n4i.jpg' }
      ],
      games: []
    }
  }
];

const PREVIEW_COMMUNITY_MAP = PREVIEW_COMMUNITY_USERS.reduce((acc, user) => {
  acc[user.uid] = user;
  return acc;
}, {});

const PREVIEW_COMMENT_THREADS = {
  'imdb:tt0944947': [
    {
      id: 'pc-got-1',
      uid: 'preview-lena',
      name: 'Lena Knox',
      photo: PREVIEW_COMMUNITY_MAP['preview-lena'].photo,
      text: 'The first season hooks me every time. The world-building still feels huge on rewatch.',
      timestamp: Date.now() - 1000 * 60 * 18,
      scope: 'global'
    },
    {
      id: 'pc-got-2',
      uid: 'preview-marcus',
      name: 'Marcus Vale',
      photo: PREVIEW_COMMUNITY_MAP['preview-marcus'].photo,
      text: 'Ned carrying the early episodes is unreal. Preview comments are read-only until you sign in.',
      timestamp: Date.now() - 1000 * 60 * 62,
      scope: 'global'
    },
    {
      id: 'pc-got-3',
      uid: 'preview-yara',
      name: 'Yara Bloom',
      photo: PREVIEW_COMMUNITY_MAP['preview-yara'].photo,
      text: 'The score, the tension, and the cliffhangers make this such a good test title for the comments page.',
      timestamp: Date.now() - 1000 * 60 * 140,
      scope: 'global'
    }
  ],
  'imdb:tt9362722': [
    {
      id: 'pc-spider-1',
      uid: 'preview-yara',
      name: 'Yara Bloom',
      photo: PREVIEW_COMMUNITY_MAP['preview-yara'].photo,
      text: 'The art direction alone makes this worth planning a movie night around.',
      timestamp: Date.now() - 1000 * 60 * 95,
      scope: 'global'
    }
  ]
};

function startPreviewFromLanding() {
  if (window.location.hash !== '#preview') {
    window.location.hash = 'preview';
    return;
  }
  enterPreviewMode();
}

function setDefaultMyListsWatchingView() {
  activeSection = "shows";
  activeTab = "watching";
  searchQuery = "";
  viewingUser = null;
  friendViewData = null;
  const navMyList = document.getElementById('nav-mylist');
  const navCommunity = document.getElementById('nav-community');
  const navDiscover = document.getElementById('nav-discover');
  const navGamesDiscover = document.getElementById('nav-games-discover');
  if (navMyList) navMyList.classList.add('active');
  if (navCommunity) navCommunity.classList.remove('active');
  if (navDiscover) navDiscover.classList.remove('active');
  if (navGamesDiscover) navGamesDiscover.classList.remove('active');
  setMainNavVisibility('mylist');
}

function showLandingPage() {
  document.body.classList.remove('preview-mode');
  const login = document.getElementById("login-screen");
  const app = document.getElementById("app-container");
  const headerBtn = document.getElementById('preview-header-signin');
  if (login) login.style.display = "flex";
  if (app) app.style.display = "none";
  if (headerBtn) headerBtn.style.display = 'none';
  window.scrollTo({ top: 0, behavior: "auto" });
}

function enterPreviewMode() {
  document.body.classList.add('preview-mode');
  const login = document.getElementById("login-screen");
  const app = document.getElementById("app-container");
  if (login) login.style.display = "none";
  if (app) app.style.display = "block";
  const headerBtn = document.getElementById('preview-header-signin');
  if (headerBtn) headerBtn.style.display = 'inline-flex';
  viewingUser = null;
  friendViewData = null;
  data = JSON.parse(JSON.stringify(DEMO_DATA));
  ownDataCache = cloneListData(data);
  userProfile = normalizeUserProfile({
    name: 'Preview User',
    photo: 'https://ui-avatars.com/api/?name=Preview+User&background=1c1535&color=a78bfa',
    bio: 'Testing ScreenList in preview mode. Build your shelves, rate titles, pin favorites, and customize your profile.',
    pinnedFavorites: {
      overallMedia: [
        { id: '569094', source: 'tmdb', type: 'movie', title: 'Spider-Man: Across the Spider-Verse', image: 'https://image.tmdb.org/t/p/w500/8Vt6mWEReuy4Of61Lnj5Xj704m8.jpg', rating: '★ 9/10', meta: '2023 · Movie' },
        { id: '1399', source: 'tmdb', type: 'tv', title: 'Game of Thrones', image: 'https://image.tmdb.org/t/p/w500/u3bZgnGQ9T01sWNhyveQz0wH0Hl.jpg', rating: '★ 9/10', meta: '2011 · TV / Anime' },
        { id: '209867', source: 'tmdb', type: 'tv', title: 'Frieren: Beyond Journey’s End', image: 'https://image.tmdb.org/t/p/w500/dqZENchTd7lp5zht7BdlqM7RBhN.jpg', rating: '★ 9/10', meta: '2023 · TV / Anime' }
      ],
      movies: [{ id: '569094', source: 'tmdb', type: 'movie', title: 'Spider-Man: Across the Spider-Verse', image: 'https://image.tmdb.org/t/p/w500/8Vt6mWEReuy4Of61Lnj5Xj704m8.jpg', rating: '★ 9/10', meta: '2023' }, {}, {}],
      shows: [{ id: '1399', source: 'tmdb', type: 'tv', title: 'Game of Thrones', image: 'https://image.tmdb.org/t/p/w500/u3bZgnGQ9T01sWNhyveQz0wH0Hl.jpg', rating: '★ 9/10', meta: '2011' }, {}, {}],
      anime: [{ id: '209867', source: 'tmdb', type: 'tv', title: 'Frieren: Beyond Journey’s End', image: 'https://image.tmdb.org/t/p/w500/dqZENchTd7lp5zht7BdlqM7RBhN.jpg', rating: '★ 9/10', meta: '2023' }, {}, {}],
      games: [{ id: '3498', source: 'rawg', type: 'game', title: 'Grand Theft Auto V', image: 'https://media.rawg.io/media/games/20a/20aa03a18ad10d5f05a16bc6ce0bb570.jpg', rating: '★ 10/10', meta: '2013' }, {}, {}],
      singlePlayerGames: [{ id: '3498', source: 'rawg', type: 'game', title: 'Grand Theft Auto V', image: 'https://media.rawg.io/media/games/20a/20aa03a18ad10d5f05a16bc6ce0bb570.jpg', rating: '★ 10/10', meta: '2013' }, {}, {}],
      actors: [{ id: '31', source: 'tmdb', type: 'person', title: 'Tom Hanks', image: 'https://image.tmdb.org/t/p/w500/xndWFsBlClOJFRdhSt4NBwiPq2o.jpg', rating: 'Favorite', meta: 'TMDB person' }, {}, {}],
      directors: [{ id: '488', source: 'tmdb', type: 'person', title: 'Steven Spielberg', image: 'https://image.tmdb.org/t/p/w500/tZxcg19YQ3e8fJ0pOs7hjlnmmr6.jpg', rating: 'Favorite', meta: 'TMDB person' }, {}, {}]
    },
    showcaseFavorites: {
      fictionalCharacters: [{ name: 'Miles Morales', image: '', rating: 'Favorite' }, {}, {}],
      musicArtists: [{ name: 'The Weeknd', image: '', rating: 'Favorite' }, {}, {}]
    },
    socialLinks: getDefaultSocialLinks(),
    uid: 'preview-user'
  });
  applyProfile();
  setDefaultMyListsWatchingView();
  render();
  window.scrollTo({ top: 0, behavior: "auto" });
}

function exitPreviewMode() {
  document.body.classList.remove('preview-mode');
  const headerBtn = document.getElementById('preview-header-signin');
  if (headerBtn) headerBtn.style.display = 'none';
  if (window.location.hash === '#preview') {
    history.replaceState(null, '', window.location.pathname + window.location.search);
  }
}

function syncSignedOutRoute() {
  if (currentUser) return;
  if (window.location.hash === '#preview') enterPreviewMode();
  else showLandingPage();
}

function getPreviewCommunityUser(uid) {
  return PREVIEW_COMMUNITY_MAP[uid] || null;
}

function getPreviewCommentsForMedia(mediaKey) {
  return (PREVIEW_COMMENT_THREADS[mediaKey] || []).map(comment => ({ ...comment }));
}

function renderPreviewCommunityUsers(users, emptyTitle, emptyCopy) {
  const grid = document.getElementById('friends-grid');
  const badge = document.getElementById('friends-count-badge');
  if (!grid || !badge) return;
  if (!users || users.length === 0) {
    badge.textContent = '';
    grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">👥</div>
      <p style="color:#7a6f99;font-size:14px;">${escHtml(emptyTitle)}</p>
      <p class="friends-empty-sub">${escHtml(emptyCopy)}</p>
    </div>`;
    return;
  }
  badge.textContent = '(' + users.length + ')';
  grid.innerHTML = users.map(user => `
    <div class="user-card friend-list-card" style="justify-content:space-between;">
      <div class="friend-card-main" style="display:flex;align-items:center;gap:14px;flex:1;min-width:0;cursor:pointer;" onclick="openPreviewCommunityProfile('${user.uid}')">
        <img class="user-card-avatar" src="${user.photo}" alt="">
        <div class="friend-card-copy" style="min-width:0;">
          <div class="user-card-name">${renderDisplayNameHTML(user, 'Preview User')}</div>
          <div class="user-card-stats">${escHtml(user.stats || user.findStats || 'Preview profile')}</div>
        </div>
      </div>
      <div class="friend-actions-group">
        <button class="friend-action-btn friend-mobile-list-btn" type="button" onclick="event.stopPropagation(); openPreviewCommunityProfile('${user.uid}')">Screen List</button>
        <button class="friend-action-btn friend-profile-btn friend-mobile-profile-btn" type="button" onclick="event.stopPropagation(); openPreviewUserProfile('${user.uid}')">Profile</button>
        <button class="friend-action-btn friend-profile-btn friend-profile-desktop-btn" type="button" onclick="event.stopPropagation(); openPreviewUserProfile('${user.uid}')">Profile</button>
        <button class="friend-action-btn friend-pending-btn" type="button" disabled>Preview</button>
      </div>
    </div>
  `).join('');
}

function openPreviewCommunityProfile(uid) {
  const user = getPreviewCommunityUser(uid);
  if (!user) {
    showToast("Preview profile unavailable");
    return;
  }
  viewingUser = { uid: user.uid, name: user.name, photo: user.photo, preview: true };
  friendViewData = cloneListData(user.listData);
  clearListSearch();
  const communityView = document.getElementById('community-view');
  const myListView = document.getElementById('mylist-view');
  const myListHeader = document.getElementById('mylist-header');
  const addBtn = document.getElementById('add-btn');
  const bannerArea = document.getElementById('viewing-banner-area');
  const mainNav = document.querySelector('.main-nav');
  const navMyList = document.getElementById('nav-mylist');
  const navCommunity = document.getElementById('nav-community');
  const navDiscover = document.getElementById('nav-discover');
  const navGamesDiscover = document.getElementById('nav-games-discover');
  if (communityView) communityView.style.display = 'none';
  if (myListView) myListView.style.display = 'block';
  if (myListHeader) myListHeader.style.display = 'block';
  if (addBtn) addBtn.style.display = 'none';
  if (mainNav) mainNav.style.display = 'flex';
  if (navMyList) navMyList.classList.add('active');
  if (navCommunity) navCommunity.classList.remove('active');
  if (navDiscover) navDiscover.classList.remove('active');
  if (navGamesDiscover) navGamesDiscover.classList.remove('active');
  if (bannerArea) {
    bannerArea.innerHTML = `<div class="viewing-banner">
      <span class="viewing-banner-text">
        <img src="${user.photo}">
        Viewing <span class="viewing-banner-name">${renderDisplayNameHTML(user, 'Preview User', 'creator-name-soft')}</span>'s preview list
      </span>
      <div class="viewing-banner-actions">
        <button class="back-btn profile-view-btn" onclick="openPreviewUserProfile('${user.uid}')">View Profile</button>
        <button class="back-btn" onclick="backToMyList()">← Back to My List</button>
      </div>
    </div>`;
  }
  const initialView = chooseInitialListView(friendViewData);
  activeSection = initialView.section;
  activeTab = initialView.tab;
  render();
}

// Load from Firestore
async function load() {
  if (!DOC_REF) return;
  try {
    const snap = await DOC_REF.get();
    if (snap.exists) {
      const d = snap.data();
      data = normalizeListData({
        shows: d.shows ? JSON.parse(d.shows) : [],
        movies: d.movies ? JSON.parse(d.movies) : [],
        anime: d.anime ? JSON.parse(d.anime) : [],
        games: d.games ? JSON.parse(d.games) : []
      });
    } else {
      data = getEmptyListData();
    }
    data = await autoSortAnimeBuckets(data, true);
    ownDataCache = cloneListData(data);
    activeSection = "shows";
    activeTab = "watching";
  } catch(e) {
    console.error("Load failed:", e);
    // Fallback to localStorage
    try {
      const raw = localStorage.getItem("watchlist-tracker-data");
      if (raw) data = normalizeListData(JSON.parse(raw));
      activeSection = "shows";
      activeTab = "watching";
      ownDataCache = cloneListData(data);
    } catch(e2) {}
  }
}

// Save to Firestore (debounced)
function save() {
  if (!DOC_REF || viewingUser) return;
  ownDataCache = cloneListData(data);
  // Save to localStorage as backup
  localStorage.setItem("watchlist-tracker-data", JSON.stringify(data));
  if (currentUser) localStorage.setItem("screenlist-own-data-backup-" + currentUser.uid, JSON.stringify(data));
  // Debounce Firestore writes
  clearTimeout(saveTimeout);
  saveTimeout = setTimeout(async () => {
    try {
      await DOC_REF.set({
        shows: JSON.stringify(data.shows),
        movies: JSON.stringify(data.movies),
        anime: JSON.stringify(data.anime),
        games: JSON.stringify(data.games),
        updatedAt: firebase.firestore.FieldValue.serverTimestamp()
      });
    } catch(e) {
      console.error("Save failed:", e);
    }
  }, 500);
}

// Render
function render() {
  const visibleData = getVisibleListData();
  const items = Array.isArray(visibleData[activeSection]) ? visibleData[activeSection] : [];
  if (activeSection === "movies" && activeTab === "watching") activeTab = "planned";
  const stateKey = getSortStateKey();
  const activeSortKey = getActiveSortKey();
  const baseFiltered = items
    .filter(i => i.status === activeTab)
    .filter(i => !searchQuery || (i.title || '').toLowerCase().includes(searchQuery.toLowerCase()));
  const filtered = applySortOrder(baseFiltered, activeSortKey, stateKey);

  const isPreview = document.body.classList.contains('preview-mode');
  const previewCap = 2;
  const previewCount = getPreviewItemCount();


  document.getElementById("shows-count").textContent = visibleData.shows.length;
  document.getElementById("anime-count").textContent = visibleData.anime.length;
  document.getElementById("movies-count").textContent = visibleData.movies.length;
  document.getElementById("games-count").textContent = visibleData.games.length;

  // Tab counts
  document.getElementById("count-live").textContent = items.filter(i => i.status === "live").length;
  document.getElementById("count-watching").textContent = items.filter(i => i.status === "watching").length;
  document.getElementById("count-planned").textContent = items.filter(i => i.status === "planned").length;
  document.getElementById("count-watched").textContent = items.filter(i => i.status === "watched").length;
  document.getElementById("count-paused").textContent = items.filter(i => i.status === "paused").length;
  document.getElementById("count-dropped").textContent = items.filter(i => i.status === "dropped").length;

  // Add button label
  document.getElementById("add-btn").textContent = `+ Add ${activeSection === "anime" ? "Anime" : activeSection === "shows" ? "Show" : activeSection === "movies" ? "Movie" : "Game"}`;

  // Section buttons
  document.querySelectorAll(".section-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.section === activeSection);
  });
  // Tab buttons
  document.querySelectorAll(".tab-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.tab === activeTab);
    if (b.dataset.tab === "live") {
      b.style.display = activeSection === "games" ? "" : "none";
    }
    if (b.dataset.tab === "watching") {
      b.style.display = activeSection === "movies" ? "none" : "";
      b.childNodes[0].textContent = activeSection === "games" ? "Playing" : "Watching";
    }
    if (b.dataset.tab === "planned") {
      b.childNodes[0].textContent = activeSection === "games" ? "Backlog" : "Plan to Watch";
    }
    if (b.dataset.tab === "watched") {
      b.childNodes[0].textContent = activeSection === "games" ? "Completed" : "Watched";
    }
  });
  const grid = document.getElementById("cards-grid");
  const empty = document.getElementById("empty-state");
  const emptySub = empty.querySelector(".empty-sub");
  const emptyCta = document.getElementById("empty-cta");

  // Inject / update sort button
  let sortBtn = document.getElementById('sort-dropdown-btn');
  if (!sortBtn) {
    sortBtn = document.createElement('button');
    sortBtn.id = 'sort-dropdown-btn';
    sortBtn.className = 'btn-secondary sort-btn';
    sortBtn.onclick = toggleSortDropdown;
    const toolbarRight = document.querySelector('.toolbar-right');
    if (toolbarRight) toolbarRight.insertBefore(sortBtn, toolbarRight.firstChild);
  }
  const isDefaultSort = activeSortKey === DEFAULT_SORT;
  const sortLabel = SORT_OPTIONS.find(o => o.key === activeSortKey)?.label || 'Sort';
  sortBtn.title = sortLabel;
  sortBtn.innerHTML = `<span class="sort-btn-icon${isDefaultSort ? '' : ' sort-active'}">⇅</span><span class="sort-btn-label">${isDefaultSort ? '' : sortLabel}</span>`;

  if (filtered.length === 0) {
    grid.innerHTML = "";
    empty.style.display = "block";
    document.getElementById("empty-icon").textContent = getSectionIcon(activeSection);
    const statusLabel = activeTab === "planned" ? "planned" : activeTab;
    const sectionLabel = getSectionLabel(activeSection);
    document.getElementById("empty-text").textContent = `No ${statusLabel} ${sectionLabel} yet`;
    if (emptySub) {
      emptySub.textContent = searchQuery
        ? "No matches for your search. Try a shorter title or clear the search field."
        : viewingUser
          ? "This list is quiet in this section right now."
          : "Start building this shelf with something you want to track.";
    }
    if (emptyCta) {
      emptyCta.style.display = (!viewingUser && !searchQuery) ? "" : "none";
      emptyCta.textContent = `Add your first ${getSectionLabel(activeSection, true)}`;
    }
    return;
  }

  empty.style.display = "none";

  if (activeSortKey === 'custom') {
    grid.innerHTML = filtered.map(item => renderCard(item, true)).join("");
  } else {
    grid.innerHTML = filtered.map(item => renderCard(item)).join("");
  }

  refreshVisibleCommentCounts();

  // Restore open episode lists and seasons
  Object.keys(openStates).forEach(key => {
    if (!openStates[key]) return;
    if (key.startsWith('ep-')) {
      const id = key.slice(3);
      const list = document.getElementById('ep-list-' + id);
      const arrow = document.getElementById('ep-arrow-' + id);
      const label = document.getElementById('ep-label-' + id);
      if (list) { setEpisodesExpanded(list, true, true); }
      if (arrow) { arrow.classList.add('open'); }
      if (label) { label.textContent = 'Hide Episodes'; }
    } else if (key.startsWith('s-')) {
      const el = document.getElementById('s-eps-' + key.slice(2));
      const arrow = document.getElementById('s-arrow-' + key.slice(2));
      if (el) { el.style.display = 'block'; }
      if (arrow) { arrow.classList.add('open'); }
    }
  });
}

function renderStars(rating, itemId, prefix, size) {
  size = size || 14;
  if (viewingUser) {
    let html = `<div class="stars">`;
    for (let s = 1; s <= 10; s++) {
      html += `<span class="star-btn ${s <= rating ? 'lit' : ''}" style="font-size:${size}px;cursor:default;">★</span>`;
    }
    if (rating > 0) html += `<span class="star-label">${rating}</span>`;
    html += `</div>`;
    return html;
  }
  let html = `<div class="stars" data-item-id="${itemId}" data-prefix="${prefix}"
    ontouchstart="starsTouchStart(event)"
    ontouchmove="starsTouchMove(event)"
    ontouchend="starsTouchEnd(event)">`;
  for (let s = 1; s <= 10; s++) {
    html += `<button class="star-btn ${s <= rating ? 'lit' : ''}" style="font-size:${size}px"
      onclick="event.stopPropagation();rate('${itemId}','${prefix}',${s})"
      onmouseenter="hoverStars(this,${s})" onmouseleave="unhoverStars(this,${rating})">★</button>`;
  }
  if (rating > 0) html += `<span class="star-label">${rating}</span>`;
  html += `</div>`;
  return html;
}

function renderCard(item, isDraggable) {
  const type = isShowSection(activeSection) ? "show" : activeSection === "movies" ? "movie" : "game";
  const mediaKey = getMediaKey(item);
  const commentCount = isPreviewMode() && !currentUser
    ? getPreviewCommentsForMedia(mediaKey).length
    : getCachedCommentCount(mediaKey);
  const coverStyle = item.cover
    ? `background-image:url('${item.cover}');background-size:cover;background-position:center;`
    : "";
  const coverClass = item.cover ? "card-cover" : "card-cover no-img";
  const emoji = type === "show" ? getSectionIcon(activeSection) : type === "movie" ? "🎬" : "🎮";
  const friendAlreadyAdded = viewingUser && myData ? isDuplicateTitleInList(item.title, activeSection, myData) : false;

  let watchedCount = 0, totalEps = 0, progress = 0;
  if (type === "show") {
    totalEps = (item.episodes || []).length;
    watchedCount = (item.episodes || []).filter(e => e.watched).length;
    progress = totalEps > 0 ? (watchedCount / totalEps) * 100 : 0;
  }

  const statusPill = (s, label) => {
    let cls = "status-pill";
    if (item.status === s) cls += ` ${s}-active`;
    return `<button class="${cls}" data-status="${s}" onclick="changeStatus('${item.id}','${s}')">${label}</button>`;
  };

  let episodeToggleButton = "";
  let episodeSection = "";
  if (type === "show") {
    episodeToggleButton = `
      <button class="ep-toggle-bar card-footer-btn" onclick="toggleEpisodes('${item.id}')">
        <span id="ep-label-${item.id}">Show Episodes</span>
        <span class="ep-arrow" id="ep-arrow-${item.id}">&#9662;</span>
      </button>
    `;
    episodeSection = `
      <div class="ep-list" id="ep-list-${item.id}">
        <div class="ep-list-inner">
        ${!viewingUser ? `<div class="ep-actions">
          <div style="display:flex;gap:8px;">
            <button class="btn-secondary btn-sm" onclick="markAllEps('${item.id}',true)">Mark All Watched</button>
            <button class="btn-secondary btn-sm" onclick="markAllEps('${item.id}',false)">Clear All</button>
          </div>
          <div class="edit-ep-row" id="edit-ep-${item.id}">
            <button class="edit-ep-link" onclick="showEditEp('${item.id}')">Edit episode count</button>
          </div>
        </div>` : ''}
        <div class="ep-scroll">
          ${renderEpisodeList(item)}
        </div>
        </div>
      </div>
    `;
  }

  const dragAttrs = isDraggable
    ? `draggable="true" ondragstart="onCardDragStart(event,'${item.id}')" ondragover="onCardDragOver(event)" ondragleave="onCardDragLeave(event)" ondrop="onCardDrop(event,'${item.id}')"`
    : '';
  return `
    <div class="card ${type === "show" ? "show-card" : ""} ${viewingUser ? "friend-view-card" : ""}${isDraggable ? ' card-draggable' : ''}" id="card-${item.id}" ${dragAttrs}>
      <div class="card-header">
        <div class="${coverClass}" style="${coverStyle}">
          ${!item.cover ? emoji : ''}
        </div>
        <div class="card-info">
          <div class="card-title-row">
            <div class="card-title">${escHtml(item.title)}${item.imdbId ? ` <a href="https://www.imdb.com/title/${item.imdbId}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="display:inline-flex;align-items:center;text-decoration:none;vertical-align:middle;">
            <span class="media-link-badge">IMDb</span>
          </a>` : ''}${activeSection === 'movies' ? `<button class="letterboxd-badge-btn" onclick="event.stopPropagation();openLetterboxd('${item.id}')" title="Letterboxd">
            <span class="letterboxd-badge">
              <svg viewBox="0 0 24 10" aria-hidden="true" fill="none">
                <circle cx="6" cy="5" r="4" fill="#FF8000"></circle>
                <circle cx="12" cy="5" r="4" fill="#00E054"></circle>
                <circle cx="18" cy="5" r="4" fill="#40BCF4"></circle>
              </svg>
            </span>
          </button>` : ''}${activeSection === 'games' ? (item.metacriticSlug ? `<a href="https://www.metacritic.com/game/${item.metacriticSlug}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="display:inline-flex;align-items:center;text-decoration:none;vertical-align:middle;margin-left:6px;">
            <span class="media-link-badge">Metacritic</span>
          </a>` : `<a href="https://www.metacritic.com/search/${encodeURIComponent(item.title)}/" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="display:inline-flex;align-items:center;text-decoration:none;vertical-align:middle;margin-left:6px;">
            <span class="media-link-badge">Metacritic</span>
          </a>`) : ''}</div>
            ${!viewingUser ? `<button class="delete-btn" onclick="deleteItem('${item.id}')" title="Delete">×</button>` : `<button class="friend-card-add-btn${friendAlreadyAdded ? ' added' : ''}" data-friend-item-id="${escHtml(item.id)}" onclick="event.stopPropagation();openFriendAddModal(this.dataset.friendItemId, this)" title="Add to my list">+</button>`}
          </div>
          ${item.genre ? `<div class="card-genre">${escHtml(item.genre)}</div>` : ''}
          ${!viewingUser ? `<div class="status-pills" id="status-pills-${item.id}">
            ${activeSection === 'games' ? statusPill('live', 'Live Games') : ''}
            ${activeSection === 'movies' ? '' : statusPill('watching', activeSection === 'games' ? 'Playing' : 'Watching')}
            ${statusPill('planned', activeSection === 'games' ? 'Backlog' : 'Plan to Watch')}
            ${statusPill('watched', activeSection === 'games' ? 'Completed' : 'Watched')}
            ${statusPill('paused', 'Paused')}
            ${statusPill('dropped', 'Dropped')}
          </div>` : ''}
          ${type === "show" ? `
            <div class="progress-area">
              <div class="progress-meta"><span id="progress-count-${item.id}">${watchedCount}/${totalEps} episodes</span><span id="progress-percent-${item.id}">${Math.round(progress)}%</span></div>
              <div class="progress-bar"><div class="progress-fill" id="progress-fill-${item.id}" style="width:${progress}%"></div></div>
            </div>
          ` : ''}
          <div class="rating-area">
            <div class="rating-label">Overall Rating</div>
            ${renderStars(item.rating || 0, item.id, 'overall', 14)}
          </div>
        </div>
      </div>
      <div class="card-action-row">
        <div class="card-footer-actions">
          <button class="comments-btn" onclick="event.stopPropagation();openCommentsPage('${item.id}', this)">
            <span class="comments-btn-label">Comments (<span class="comment-count" data-media-key="${escAttr(mediaKey)}">${commentCount}</span>)</span>
          </button>
          ${episodeToggleButton}
        </div>
      </div>
      ${episodeSection}
    </div>
  `;
}

function renderEpisodeList(item) {
  const eps = item.episodes || [];
  const hasSeasons = eps.some(e => e.seasonNum);
  if (!hasSeasons || new Set(eps.map(e => e.seasonNum)).size <= 1) {
    // No season data or single season: flat list
    return eps.map(ep => renderSingleEp(item.id, ep)).join("");
  }
  // Group by season
  const seasons = {};
  eps.forEach(ep => {
    const s = ep.seasonNum || 1;
    if (!seasons[s]) seasons[s] = [];
    seasons[s].push(ep);
  });
  return Object.keys(seasons).sort((a,b) => a - b).map(sNum => {
    const sEps = seasons[sNum];
    const sWatched = sEps.filter(e => e.watched).length;
    return `<div class="season-block">
      <div class="season-header" onclick="toggleSeason('${item.id}',${sNum})">
        <div class="season-header-left">
          <span class="season-arrow" id="s-arrow-${item.id}-${sNum}">▼</span>
          <span class="season-title">Season ${sNum}</span>
          <span class="season-progress" id="season-progress-${item.id}-${sNum}">${sWatched}/${sEps.length}</span>
          ${(item.seasonRatings && item.seasonRatings[sNum]) ? `<span style="color:#f59e0b;font-size:11px;margin-left:4px;">★ ${item.seasonRatings[sNum]}</span>` : ''}
        </div>
        ${!viewingUser ? `<button class="edit-ep-link" onclick="event.stopPropagation();markSeasonEps('${item.id}',${sNum},${sWatched < sEps.length})" style="text-decoration:none;font-size:11px;">
          ${sWatched < sEps.length ? 'Mark all' : 'Clear all'}
        </button>` : ''}
      </div>
      <div class="season-eps" id="s-eps-${item.id}-${sNum}" style="display:none">
        <div style="padding:6px 8px 10px;display:flex;align-items:center;gap:8px;">
          <span style="font-size:11px;color:#7a6f99;">Season Rating</span>
          ${renderStars((item.seasonRatings && item.seasonRatings[sNum]) || 0, item.id, 'season:' + sNum, 13)}
        </div>
        ${sEps.map(ep => renderSingleEp(item.id, ep)).join("")}
      </div>
    </div>`;
  }).join("");
}

function renderSingleEp(itemId, ep) {
  const r = ep.rating || 0;
  if (viewingUser) {
    return `<div class="ep-row ${ep.watched ? 'watched-ep' : ''}">
      <div class="ep-left">
        <span class="ep-check ${ep.watched ? 'checked' : ''}" style="cursor:default;">
          ${ep.watched ? '✓' : ''}
        </span>
        <span class="ep-name">${ep.epNum || ep.number}${ep.title ? ' — ' + escHtml(ep.title) : ''}</span>
      </div>
      <span class="ep-rating-btn ${r ? 'has-rating' : ''}" style="cursor:default;">
        ★${r ? ' ' + r : ''}
      </span>
    </div>`;
  }
  return `<div class="ep-row ${ep.watched ? 'watched-ep' : ''}" id="ep-row-${ep.id}">
    <div class="ep-left">
      <button class="ep-check ${ep.watched ? 'checked' : ''}" onclick="toggleEp('${itemId}','${ep.id}')">
        ${ep.watched ? '✓' : ''}
      </button>
      <span class="ep-name">${ep.epNum || ep.number}${ep.title ? ' — ' + escHtml(ep.title) : ''}</span>
    </div>
    <button class="ep-rating-btn ${r ? 'has-rating' : ''}" onclick="event.stopPropagation();openEpRating('${itemId}','${ep.id}')">
      ★${r ? ' ' + r : ''}
    </button>
  </div>`;
}

// Episode rating popup
let activePopup = null;

function openEpRating(itemId, epId) {
  closeEpRating();
  const row = document.getElementById('ep-row-' + epId);
  if (!row) return;
  const item = data[activeSection].find(i => i.id === itemId);
  const ep = item ? (item.episodes || []).find(e => e.id === epId) : null;
  const currentRating = ep ? (ep.rating || 0) : 0;
  const popup = document.createElement('div');
  popup.className = 'ep-rating-popup';
  popup.id = 'ep-rating-popup';
  popup.dataset.itemId = itemId;
  popup.dataset.epId = epId;
  popup.dataset.hovered = '0';
  let html = '';
  for (let s = 1; s <= 10; s++) {
    html += `<button class="star-btn ${s <= currentRating ? 'lit' : ''}" data-star="${s}" onclick="event.stopPropagation();rateEpPopup('${itemId}','${epId}',${s})"
      onmouseenter="hoverStars(this,${s})" onmouseleave="unhoverStars(this,${currentRating})">★</button>`;
  }
  if (currentRating > 0) {
    html += `<button style="background:none;border:none;color:#7a6f99;font-size:11px;cursor:pointer;margin-left:4px;" onclick="event.stopPropagation();rateEpPopup('${itemId}','${epId}',0)">✕</button>`;
  }
  popup.innerHTML = html;
  // Touch scrub support
  popup.addEventListener('touchmove', function(e) {
    e.preventDefault();
    const touch = e.touches[0];
    const el = document.elementFromPoint(touch.clientX, touch.clientY);
    if (el && el.dataset && el.dataset.star) {
      const val = parseInt(el.dataset.star);
      popup.dataset.hovered = val;
      popup.querySelectorAll('.star-btn').forEach((b, i) => {
        b.style.color = (i + 1) <= val ? '#f59e0b' : '#443d60';
        b.style.transform = (i + 1) <= val ? 'scale(1.2)' : 'scale(1)';
      });
    }
  }, { passive: false });
  popup.addEventListener('touchend', function(e) {
    const val = parseInt(popup.dataset.hovered);
    if (val > 0) {
      rateEpPopup(popup.dataset.itemId, popup.dataset.epId, val);
    }
  });
  row.appendChild(popup);
  activePopup = popup;
  setTimeout(() => document.addEventListener('click', closeEpRating, { once: true }), 10);
}

function closeEpRating() {
  const popup = document.getElementById('ep-rating-popup');
  if (popup) popup.remove();
  activePopup = null;
}

function rateEpPopup(itemId, epId, score) {
  const item = data[activeSection].find(i => i.id === itemId);
  if (!item) return;
  const ep = (item.episodes || []).find(e => e.id === epId);
  if (!ep) return;
  preserveEpisodeScroll(itemId, () => {
    ep.rating = (ep.rating === score && score !== 0) ? 0 : score;
    closeEpRating();
    save(); render();
  });
  // Confirmation animation on the episode's rating button — single shadow, GPU-friendly
  if (score > 0) {
    const t = Math.pow(score / 10, 1.3);
    const peakScale = 1.25 + t * 0.5;
    const glow = 5 + t * 13;
    const glowAlpha = 0.55 + t * 0.45;
    const glowR = Math.round(251 - t * 15);
    const glowG = Math.round(191 - t * 119);
    const glowB = Math.round(36 + t * 117);
    const peakFilter = `drop-shadow(0 0 ${glow}px rgba(${glowR},${glowG},${glowB},${glowAlpha}))`;

    requestAnimationFrame(() => {
      const row = document.getElementById('ep-row-' + epId);
      const btn = row && row.querySelector('.ep-rating-btn');
      if (!btn) return;
      btn.style.willChange = 'transform, filter';
      const anim = btn.animate([
        { transform: 'scale(1)', filter: 'none' },
        { transform: `scale(${peakScale})`, filter: peakFilter, offset: 0.4 },
        { transform: 'scale(1)', filter: 'none' }
      ], { duration: 400 + t * 220, easing: 'ease-out' });
      anim.onfinish = () => { btn.style.willChange = ''; };
      if (score === 10) spawnPerfectBurst(btn);
    });
  }
}

function toggleSeason(itemId, sNum) {
  const el = document.getElementById('s-eps-' + itemId + '-' + sNum);
  const arrow = document.getElementById('s-arrow-' + itemId + '-' + sNum);
  if (!el) return;
  const open = el.style.display !== 'none';
  el.style.display = open ? 'none' : 'block';
  arrow.classList.toggle('open', !open);
  openStates['s-' + itemId + '-' + sNum] = !open;
}

function markSeasonEps(itemId, sNum, val) {
  const item = data[activeSection].find(i => i.id === itemId);
  if (!item) return;
  preserveEpisodeScroll(itemId, () => {
    item.episodes.forEach(e => { if (e.seasonNum === sNum) e.watched = val; });
    const allWatched = item.episodes.every(e => e.watched);
    const anyWatched = item.episodes.some(e => e.watched);
    if (allWatched) item.status = "watched";
    else if (anyWatched) item.status = "watching";
    else item.status = "planned";
    save(); render();
  });
}

function escHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
function escAttr(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function getLetterboxdDirectUrl(item) {
  if (!item || !item.tmdbId) return '';
  return `https://letterboxd.com/tmdb/${item.tmdbId}`;
}

async function backfillLetterboxdForItem(item) {
  if (!item || activeSection !== 'movies' || item.tmdbId || !item.title) return false;
  try {
    const res = await fetchTmdbProxy('search/movie', { query: item.title });
    const json = await res.json();
    const results = json.results || [];
    let match = null;
    if (item.year) {
      match = results.find(r => ((r.release_date || '').slice(0, 4) === String(item.year)));
    }
    if (!match) match = results[0];
    if (!match || !match.id) return false;
    item.tmdbId = String(match.id);
    save();
    return true;
  } catch (e) {
    console.error('Letterboxd backfill failed:', e);
    return false;
  }
}

async function openLetterboxd(itemId) {
  const visibleData = getVisibleListData();
  const item = (visibleData[activeSection] || []).find(i => i.id === itemId);
  if (!item) return;

  if (activeSection !== 'movies') {
    showToast("OOPS! Letterboxd does not have this title");
    return;
  }

  if (!item.tmdbId) {
    const ok = await backfillLetterboxdForItem(item);
    if (!ok || !item.tmdbId) {
      showToast("OOPS! Letterboxd does not have this title");
      return;
    }
    render();
  }

  window.open(getLetterboxdDirectUrl(item), '_blank', 'noopener');
}

function showToast(message) {
  const existing = document.querySelector('.app-toast');
  if (existing) existing.remove();
  const toast = document.createElement('div');
  toast.className = 'app-toast';
  if (message === "OOPS! Letterboxd does not have this title" || message === "this title is already added to your library silly!") toast.classList.add('letterboxd-error');
  toast.textContent = message;
  document.body.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 240);
  }, 1500);
}

function clearListSearch() {
  searchQuery = "";
  const input = document.querySelector(".search-input");
  if (input) input.value = "";
}

function chooseInitialListView(listData) {
  const sectionOrder = ["movies", "shows", "anime", "games"];
  const statusOrderBySection = {
    shows: ["watching", "planned", "watched", "paused", "dropped"],
    anime: ["watching", "planned", "watched", "paused", "dropped"],
    movies: ["planned", "watched", "paused", "dropped"],
    games: ["live", "watching", "planned", "watched", "paused", "dropped"]
  };

  for (const section of sectionOrder) {
    const statuses = statusOrderBySection[section];
    const items = Array.isArray(listData[section]) ? listData[section] : [];
    for (const status of statuses) {
      if (items.some(item => item.status === status)) {
        return { section, tab: status };
      }
    }
  }
  return { section: "movies", tab: "planned" };
}

// Actions
function switchSection(s) {
  activeSection = s;
  activeTab = getDefaultTabForSection(s);
  closeSortDropdown();
  render();
  persistUiState();
}
function switchTab(t) {
  activeTab = t;
  closeSortDropdown();
  render();
  persistUiState();
}
function onSearch(q) {
  searchQuery = q;
  render();
}

function changeStatus(id, status) {
  const items = data[activeSection];
  const item = items.find(i => i.id === id);
  if (!item) return;
  item.status = status;
  if (isShowSection(activeSection) && Array.isArray(item.episodes)) {
    if (status === "watched") item.episodes.forEach(e => e.watched = true);
    if (status === "planned") item.episodes.forEach(e => e.watched = false);
  }
  save(); render();
}

function deleteItem(id) {
  const btn = event.target;
  if (btn.dataset.confirmDelete === id) {
    data[activeSection] = data[activeSection].filter(i => i.id !== id);
    save(); render();
  } else {
    btn.dataset.confirmDelete = id;
    btn.textContent = '✓';
    btn.style.color = '#ef4444';
    btn.style.fontSize = '14px';
    btn.title = 'Tap again to confirm';
    setTimeout(() => {
      btn.dataset.confirmDelete = '';
      btn.textContent = '×';
      btn.style.color = '#5c5278';
      btn.style.fontSize = '16px';
      btn.title = 'Delete';
    }, 1500);
  }
}

let _lastRate = { key: null, time: 0 };
function rate(itemId, prefix, score) {
  // Debounce: ignore identical rate within 350ms (prevents touch+click double-fire from toggling off)
  const key = itemId + '|' + prefix + '|' + score;
  const now = Date.now();
  if (_lastRate.key === key && now - _lastRate.time < 350) return;
  _lastRate = { key, time: now };

  const items = data[activeSection];
  const item = items.find(i => i.id === itemId);
  if (!item) return;
  if (prefix === "overall") {
    item.rating = item.rating === score ? 0 : score;
  } else if (prefix.startsWith("season:")) {
    const sNum = parseInt(prefix.slice(7));
    if (!item.seasonRatings) item.seasonRatings = {};
    item.seasonRatings[sNum] = (item.seasonRatings[sNum] === score) ? 0 : score;
  } else if (prefix.startsWith("ep:")) {
    const epId = prefix.slice(3);
    const ep = (item.episodes || []).find(e => e.id === epId);
    if (ep) ep.rating = ep.rating === score ? 0 : score;
  }
  save(); render();
  if (score > 0) playRatingAnimation(itemId, prefix);
}

function playRatingAnimation(itemId, prefix) {
  // Look up the actual score from the data so animation intensity matches
  const item = data[activeSection].find(i => i.id === itemId);
  if (!item) return;
  let score = 0;
  if (prefix === 'overall') score = item.rating || 0;
  else if (prefix.startsWith('season:')) {
    const sNum = parseInt(prefix.slice(7));
    score = (item.seasonRatings && item.seasonRatings[sNum]) || 0;
  }
  if (score < 1) return;

  // Map score (1-10) to 0-1 intensity, ramps faster at top
  const t = Math.pow(score / 10, 1.3);
  const peakScale = 1.3 + t * 0.7;       // 1.34 → 2.0
  const midScale  = 1.05 + t * 0.18;
  const glow      = 5 + t * 16;          // 5.5 → 21px
  const glowAlpha = 0.5 + t * 0.5;       // 0.53 → 1.0
  const stagger   = (0.07 - t * 0.04) * 1000;
  const duration  = 380 + t * 240;
  const isPerfect = score === 10;

  // ONE drop-shadow only — color shifts toward magenta at high scores instead of stacking shadows
  // (multiple drop-shadows are GPU-expensive and cause the stutter you saw)
  const glowR = Math.round(251 - t * 15);  // 251 → 236
  const glowG = Math.round(191 - t * 119); // 191 → 72
  const glowB = Math.round(36 + t * 117);  // 36 → 153
  const peakFilter = `drop-shadow(0 0 ${glow}px rgba(${glowR},${glowG},${glowB},${glowAlpha}))`;

  requestAnimationFrame(() => {
    const containers = document.querySelectorAll('.stars');
    containers.forEach(c => {
      if (c.dataset.itemId !== itemId || c.dataset.prefix !== prefix) return;
      const lit = [...c.querySelectorAll('.star-btn.lit')];
      lit.forEach((star, i) => {
        // Tell the browser to promote this element to its own GPU layer for the animation
        star.style.willChange = 'transform, filter';
        const anim = star.animate([
          { transform: 'scale(1)', filter: 'none' },
          { transform: `scale(${peakScale})`, filter: peakFilter, offset: 0.3 },
          { transform: `scale(${midScale})`, filter: 'none', offset: 0.6 },
          { transform: 'scale(1)', filter: 'none' }
        ], { duration, delay: i * stagger, easing: 'ease-out', fill: 'none' });
        anim.onfinish = () => { star.style.willChange = ''; };
      });

      const label = c.querySelector('.star-label');
      if (label) {
        label.style.willChange = 'transform, color';
        const lAnim = label.animate([
          { transform: 'scale(1)', color: '' },
          { transform: `scale(${1.15 + t * 0.35})`, color: '#fbbf24', offset: 0.4 },
          { transform: 'scale(1)', color: '' }
        ], { duration: 500 + t * 180, delay: 100 + t * 70, easing: 'ease-out' });
        lAnim.onfinish = () => { label.style.willChange = ''; };
      }

      if (isPerfect) spawnPerfectBurst(c);
    });
  });
}

function spawnPerfectBurst(container) {
  const burst = document.createElement('div');
  burst.style.cssText = `
    position:absolute; inset:-10px; border-radius:8px; pointer-events:none;
    background: radial-gradient(circle, rgba(251,191,36,0.45), rgba(236,72,153,0.2) 50%, transparent 70%);
    z-index:0;
  `;
  const oldPos = getComputedStyle(container).position;
  if (oldPos === 'static') container.style.position = 'relative';
  container.appendChild(burst);
  burst.animate([
    { opacity: 0, transform: 'scale(0.6)' },
    { opacity: 1, transform: 'scale(1.1)', offset: 0.3 },
    { opacity: 0, transform: 'scale(1.6)' }
  ], { duration: 700, easing: 'ease-out' }).onfinish = () => burst.remove();
}

function hoverStars(btn, val) {
  const container = btn.parentElement;
  container.querySelectorAll('.star-btn').forEach((b, i) => {
    b.style.color = (i + 1) <= val ? '#f59e0b' : '#443d60';
    b.style.transform = (i + 1) <= val ? 'scale(1.2)' : 'scale(1)';
  });
  let label = container.querySelector('.star-label');
  if (!label) {
    label = document.createElement('span');
    label.className = 'star-label';
    container.appendChild(label);
  }
  label.textContent = val;
}
function unhoverStars(btn, rating) {
  const container = btn.parentElement;
  container.querySelectorAll('.star-btn').forEach((b, i) => {
    b.style.color = (i + 1) <= rating ? '#f59e0b' : '#443d60';
    b.style.transform = 'scale(1)';
  });
  const label = container.querySelector('.star-label');
  if (label) label.textContent = rating > 0 ? rating : '';
}

// Touch scrub for overall + season star ratings
function starsTouchStart(e) {
  const c = e.currentTarget;
  c.dataset.touchStartX = e.touches[0].clientX;
  c.dataset.touchStartY = e.touches[0].clientY;
  c.dataset.scrubVal = '0';
  c.dataset.scrubbing = 'false';
}
function starsTouchMove(e) {
  const c = e.currentTarget;
  const touch = e.touches[0];
  const dx = Math.abs(touch.clientX - parseFloat(c.dataset.touchStartX || 0));
  const dy = Math.abs(touch.clientY - parseFloat(c.dataset.touchStartY || 0));
  // Need at least 10px of horizontal-dominant motion before locking in scrub mode
  // (prevents tap jitter from being misread as a scrub)
  if (c.dataset.scrubbing !== 'true') {
    if (dx < 10 || dy > dx) return;
  }
  c.dataset.scrubbing = 'true';
  e.preventDefault();
  const stars = [...c.querySelectorAll('.star-btn')];
  let val = 0;
  stars.forEach((btn, i) => {
    if (touch.clientX >= btn.getBoundingClientRect().left) val = i + 1;
  });
  if (val >= 1) {
    c.dataset.scrubVal = val;
    stars.forEach((b, i) => {
      b.style.color = (i + 1) <= val ? '#f59e0b' : '#443d60';
      b.style.transform = (i + 1) <= val ? 'scale(1.2)' : 'scale(1)';
    });
    let label = c.querySelector('.star-label');
    if (!label) {
      label = document.createElement('span');
      label.className = 'star-label';
      c.appendChild(label);
    }
    label.textContent = val;
  }
}
function starsTouchEnd(e) {
  const c = e.currentTarget;
  if (c.dataset.scrubbing !== 'true') return;
  const val = parseInt(c.dataset.scrubVal || 0);
  if (val > 0) {
    e.preventDefault();
    rate(c.dataset.itemId, c.dataset.prefix, val);
  }
  c.dataset.scrubVal = '0';
  c.dataset.scrubbing = 'false';
}

function setEpisodesExpanded(list, shouldOpen, immediate) {
  if (!list) return;
  if (list._episodesTransitionHandler) {
    list.removeEventListener('transitionend', list._episodesTransitionHandler);
    list._episodesTransitionHandler = null;
  }

  const startHeight = list.getBoundingClientRect().height;
  const content = list.querySelector('.ep-list-inner');

  if (shouldOpen) {
    list.classList.add('open');
    if (immediate) {
      list.style.height = 'auto';
      return;
    }
    list.style.height = startHeight + 'px';
    const targetHeight = content ? Math.ceil(content.getBoundingClientRect().height) : list.scrollHeight;
    void list.offsetHeight;
    list.style.height = targetHeight + 'px';
  } else {
    list.style.height = startHeight + 'px';
    void list.offsetHeight;
    list.classList.remove('open');
    if (immediate) {
      list.style.height = '0px';
      return;
    }
    list.style.height = '0px';
  }

  const onTransitionEnd = (e) => {
    if (e.propertyName !== 'height') return;
    if (shouldOpen) {
      requestAnimationFrame(() => {
        if (list.classList.contains('open')) list.style.height = 'auto';
      });
    }
    list.removeEventListener('transitionend', onTransitionEnd);
    list._episodesTransitionHandler = null;
  };

  list._episodesTransitionHandler = onTransitionEnd;
  list.addEventListener('transitionend', onTransitionEnd);
}

function toggleEpisodes(id) {
  const list = document.getElementById('ep-list-' + id);
  const arrow = document.getElementById('ep-arrow-' + id);
  const label = document.getElementById('ep-label-' + id);
  if (!list) return;
  const open = list.classList.contains('open');
  setEpisodesExpanded(list, !open, false);
  arrow.classList.toggle('open', !open);
  label.textContent = open ? 'Show Episodes' : 'Hide Episodes';
  openStates['ep-' + id] = !open;
}

function preserveEpisodeScroll(itemId, action) {
  const scrollEl = document.querySelector(`#ep-list-${itemId} .ep-scroll`);
  const scrollTop = scrollEl ? scrollEl.scrollTop : 0;
  action();
  requestAnimationFrame(() => {
    const nextScrollEl = document.querySelector(`#ep-list-${itemId} .ep-scroll`);
    if (nextScrollEl) nextScrollEl.scrollTop = scrollTop;
  });
}

function itemMatchesCurrentView(item) {
  return item.status === activeTab &&
    (!searchQuery || (item.title || '').toLowerCase().includes(searchQuery.toLowerCase()));
}

function getEpisodeProgress(item) {
  const total = (item.episodes || []).length;
  const watched = (item.episodes || []).filter(e => e.watched).length;
  return {
    total,
    watched,
    percent: total > 0 ? Math.round((watched / total) * 100) : 0
  };
}

function updateEpisodeRowState(ep) {
  const row = document.getElementById('ep-row-' + ep.id);
  if (!row) return;
  row.classList.toggle('watched-ep', !!ep.watched);
  const check = row.querySelector('.ep-check');
  if (!check) return;
  check.classList.toggle('checked', !!ep.watched);
  check.innerHTML = ep.watched ? '&#10003;' : '';
}

function updateCardProgressUI(item) {
  const progress = getEpisodeProgress(item);
  const countEl = document.getElementById('progress-count-' + item.id);
  const percentEl = document.getElementById('progress-percent-' + item.id);
  const fillEl = document.getElementById('progress-fill-' + item.id);
  if (countEl) countEl.textContent = `${progress.watched}/${progress.total} episodes`;
  if (percentEl) percentEl.textContent = `${progress.percent}%`;
  if (fillEl) fillEl.style.width = `${progress.percent}%`;
}

function updateSeasonProgressUI(item, seasonNum) {
  if (!seasonNum) return;
  const seasonEpisodes = (item.episodes || []).filter(e => e.seasonNum === seasonNum);
  const seasonProgressEl = document.getElementById(`season-progress-${item.id}-${seasonNum}`);
  if (!seasonProgressEl) return;
  const watched = seasonEpisodes.filter(e => e.watched).length;
  seasonProgressEl.textContent = `${watched}/${seasonEpisodes.length}`;
}

function updateStatusPillsUI(item) {
  const statusPills = document.querySelectorAll(`#status-pills-${item.id} .status-pill`);
  statusPills.forEach(btn => {
    const isActive = btn.dataset.status === item.status;
    ['watching-active', 'planned-active', 'watched-active', 'paused-active', 'dropped-active']
      .forEach(cls => btn.classList.remove(cls));
    if (isActive) btn.classList.add(`${item.status}-active`);
  });
}

function spawnEpisodeBurst(row) {
  const check = row.querySelector('.ep-check');
  if (!check) return;
  const burst = document.createElement('div');
  burst.className = 'episode-burst';
  const rowRect = row.getBoundingClientRect();
  const checkRect = check.getBoundingClientRect();
  burst.style.left = `${checkRect.left - rowRect.left + checkRect.width / 2}px`;
  burst.style.top = `${checkRect.top - rowRect.top + checkRect.height / 2}px`;
  row.appendChild(burst);
  burst.animate([
    { opacity: 0, transform: 'scale(0.35)' },
    { opacity: 1, transform: 'scale(1.12)', offset: 0.22 },
    { opacity: 0, transform: 'scale(1.9)' }
  ], {
    duration: 620,
    easing: 'cubic-bezier(0.16, 1, 0.3, 1)'
  }).onfinish = () => burst.remove();
}

function animateEpisodeWatchSweep(epId) {
  requestAnimationFrame(() => {
    const row = document.getElementById('ep-row-' + epId);
    if (!row) return;
    row.classList.remove('episode-watch-enter');
    row.classList.remove('episode-watch-sweep');
    row.classList.remove('episode-watch-impact');
    row.classList.remove('episode-watch-glow');
    void row.offsetWidth;
    row.classList.add('episode-watch-enter');
    row.classList.add('episode-watch-sweep');
    row.classList.add('episode-watch-impact');
    row.classList.add('episode-watch-glow');

    row.querySelectorAll('.episode-fill-layer').forEach(layer => layer.remove());
    const fillLayer = document.createElement('div');
    fillLayer.className = 'episode-fill-layer';
    row.appendChild(fillLayer);

    spawnEpisodeBurst(row);

    const fillAnim = fillLayer.animate([
      { clipPath: 'inset(0 99% 0 0 round 4px)' },
      { clipPath: 'inset(0 84% 0 0 round 4px)', offset: 0.16 },
      { clipPath: 'inset(0 66% 0 0 round 4px)', offset: 0.32 },
      { clipPath: 'inset(0 48% 0 0 round 4px)', offset: 0.48 },
      { clipPath: 'inset(0 29% 0 0 round 4px)', offset: 0.64 },
      { clipPath: 'inset(0 12% 0 0 round 4px)', offset: 0.8 },
      { clipPath: 'inset(0 3% 0 0 round 4px)', offset: 0.92 },
      { clipPath: 'inset(0 0 0 0 round 4px)' }
    ], {
      duration: 1120,
      easing: 'linear',
      fill: 'forwards'
    });

    fillAnim.onfinish = () => {
      fillLayer.remove();
      row.classList.remove('episode-watch-sweep');
      row.classList.remove('episode-watch-enter');
      row.classList.remove('episode-watch-impact');
      row.classList.remove('episode-watch-glow');
    };
  });
}

function toggleEp(itemId, epId) {
  const item = data[activeSection].find(i => i.id === itemId);
  if (!item) return;
  const ep = item.episodes.find(e => e.id === epId);
  if (!ep) return;
  let becameWatched = false;
  let shouldRerender = false;
  preserveEpisodeScroll(itemId, () => {
    ep.watched = !ep.watched;
    becameWatched = ep.watched;
    const allWatched = item.episodes.every(e => e.watched);
    const anyWatched = item.episodes.some(e => e.watched);
    if (allWatched) item.status = "watched";
    else if (anyWatched) item.status = "watching";
    else item.status = "planned";
    touchItem(item);
    save();
    shouldRerender = !itemMatchesCurrentView(item);
    if (shouldRerender) {
      render();
      return;
    }
    updateEpisodeRowState(ep);
    updateCardProgressUI(item);
    updateSeasonProgressUI(item, ep.seasonNum);
    updateStatusPillsUI(item);
  });
  if (becameWatched) animateEpisodeWatchSweep(epId);
}

function markAllEps(id, val) {
  const item = data[activeSection].find(i => i.id === id);
  if (!item) return;
  preserveEpisodeScroll(id, () => {
    item.episodes.forEach(e => e.watched = val);
    item.status = val ? "watched" : "planned";
    save(); render();
  });
}

function showEditEp(id) {
  const item = data[activeSection].find(i => i.id === id);
  if (!item) return;
  const el = document.getElementById('edit-ep-' + id);
  el.innerHTML = `
    <input type="number" min="1" value="${item.episodes.length}" style="width:60px;padding:4px 8px;font-size:12px;background:#0c0a1d;border:1px solid #2a2248;border-radius:4px;color:#e8e3f3;outline:none;" id="ep-count-inp-${id}">
    <button class="btn-primary btn-sm" onclick="saveEpCount('${id}')">Save</button>
    <button class="btn-secondary btn-sm" onclick="render()">Cancel</button>
  `;
}

function saveEpCount(id) {
  const item = data[activeSection].find(i => i.id === id);
  if (!item) return;
  const count = Math.max(1, parseInt(document.getElementById('ep-count-inp-' + id).value) || 1);
  const curr = item.episodes;
  preserveEpisodeScroll(id, () => {
    if (count > curr.length) {
      for (let i = curr.length; i < count; i++) {
        curr.push({ id: id + '-ep-' + (i+1), number: i+1, title: '', watched: false, rating: 0 });
      }
    } else {
      item.episodes = curr.slice(0, count);
    }
    item.totalEpisodes = count;
    save(); render();
  });
}

// TMDB Cover Search
const TMDB_PROXY_BASE = "/api/tmdb";
const RAWG_PROXY_BASE = "/api/rawg";
let selectedTmdb = null; // holds the selected item data

function buildProxyUrl(base, path, params = {}) {
  const cleanPath = String(path || '').replace(/^\/+/, '');
  const search = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return;
    search.set(key, String(value));
  });
  const query = search.toString();
  return `${base}/${cleanPath}${query ? `?${query}` : ''}`;
}

function fetchTmdbProxy(path, params = {}) {
  return fetch(buildProxyUrl(TMDB_PROXY_BASE, path, params));
}

function fetchRawgProxy(path, params = {}) {
  return fetch(buildProxyUrl(RAWG_PROXY_BASE, path, params));
}

function saveTmdbKey() {
  localStorage.removeItem("tmdb-api-key");
  renderApiKeySection();
}

function clearTmdbKey() {
  localStorage.removeItem("tmdb-api-key");
  renderApiKeySection();
}

function renderApiKeySection() {
  document.getElementById("api-key-section").innerHTML = '';
}

function doSearch() {
  if (activeSection === 'games') searchRAWG();
  else searchTMDB();
}

async function searchRAWG() {
  const query = document.getElementById("inp-tmdb-search").value.trim();
  if (!query) return;
  const resultsDiv = document.getElementById("tmdb-results");
  resultsDiv.innerHTML = '<div class="cover-search-msg">Searching...</div>';
  try {
    const res = await fetchRawgProxy('games', { search: query, page_size: 6 });
    const json = await res.json();
    const hits = (json.results || []).slice(0, 6);
    if (hits.length === 0) {
      resultsDiv.innerHTML = '<div class="cover-search-msg">No results found. Try a different search.</div>';
      return;
    }
    resultsDiv.innerHTML = '<div class="tmdb-results">' + hits.map(r => {
      const title = escHtml(r.name || '');
      const year = (r.released || '').slice(0, 4);
      const platforms = (r.platforms || []).map(p => p.platform.name).slice(0, 3).join(', ');
      const poster = r.background_image ? r.background_image : '';
      const posterThumb = poster ? `<img src="${poster}" style="width:66px;height:44px;border-radius:4px;object-fit:cover;flex-shrink:0;">` : '';
      return `<div class="tmdb-result" onclick="selectRAWG(${r.id})">
        ${posterThumb}
        <div class="tmdb-result-info">
          <div class="tmdb-result-title">${title} ${year ? '(' + year + ')' : ''}</div>
          <div class="tmdb-result-meta">${escHtml(platforms)}</div>
        </div>
      </div>`;
    }).join("") + '</div>';
  } catch(e) {
    resultsDiv.innerHTML = '<div class="cover-search-msg">Search failed.</div>';
  }
}

async function selectRAWG(id) {
  const resultsDiv = document.getElementById("tmdb-results");
  resultsDiv.innerHTML = '<div class="cover-search-msg">Loading details...</div>';
  try {
    const res = await fetchRawgProxy(`games/${id}`);
    const d = await res.json();
    const title = d.name || '';
    const cover = d.background_image || '';
    const genres = (d.genres || []).map(g => g.name).join(', ');
    const year = (d.released || '').slice(0, 4);
    const platforms = (d.platforms || []).map(p => p.platform.name).join(', ');

    selectedTmdb = { title, cover, genre: genres, year, platforms, metacriticSlug: d.slug || '' };

    resultsDiv.innerHTML = '';
    const selectedArea = document.getElementById("tmdb-selected-area");
    selectedArea.style.display = "block";
    selectedArea.innerHTML = `<div class="tmdb-selected">
      ${cover ? `<img src="${cover}" style="width:90px;height:60px;border-radius:4px;object-fit:cover;">` : ''}
      <div class="tmdb-selected-info">
        <div class="tmdb-selected-title">${escHtml(title)} ${year ? '(' + year + ')' : ''}</div>
        <div class="tmdb-selected-detail">${escHtml(genres)}</div>
        <div class="tmdb-selected-detail">${escHtml(platforms)}</div>
        <button class="tmdb-clear" onclick="clearSelection()">Clear selection</button>
      </div>
    </div>`;
    showModalStatusPicker();
    document.getElementById("tmdb-search-area").style.display = "none";
  } catch(e) {
    resultsDiv.innerHTML = '<div class="cover-search-msg">Failed to load details. Try again.</div>';
  }
}

async function searchTMDB() {
  const query = document.getElementById("inp-tmdb-search").value.trim();
  if (!query) return;
  const resultsDiv = document.getElementById("tmdb-results");
  resultsDiv.innerHTML = '<div class="cover-search-msg">Searching...</div>';
  try {
    const type = isShowSection(activeSection) ? "tv" : "movie";
    const res = await fetchTmdbProxy(`search/${type}`, { query });
    const json = await res.json();
    const hits = (json.results || []).filter(r => r.poster_path).slice(0, 6);
    if (hits.length === 0) {
      resultsDiv.innerHTML = '<div class="cover-search-msg">No results found. Try a different search.</div>';
      return;
    }
    resultsDiv.innerHTML = '<div class="tmdb-results">' + hits.map(r => {
      const title = escHtml(r.title || r.name || '');
      const year = (r.release_date || r.first_air_date || '').slice(0, 4);
      const overview = escHtml((r.overview || '').slice(0, 80)) + (r.overview && r.overview.length > 80 ? '...' : '');
      const poster = `https://image.tmdb.org/t/p/w92${r.poster_path}`;
      return `<div class="tmdb-result" onclick="selectTMDB(${r.id})">
        <img src="${poster}">
        <div class="tmdb-result-info">
          <div class="tmdb-result-title">${title} ${year ? '(' + year + ')' : ''}</div>
          <div class="tmdb-result-meta">${overview}</div>
        </div>
      </div>`;
    }).join("") + '</div>';
  } catch(e) {
    resultsDiv.innerHTML = '<div class="cover-search-msg">Search failed. Try again.</div>';
  }
}

async function selectTMDB(id) {
  const type = isShowSection(activeSection) ? "tv" : "movie";
  const resultsDiv = document.getElementById("tmdb-results");
  resultsDiv.innerHTML = '<div class="cover-search-msg">Loading details...</div>';
  try {
    const res = await fetchTmdbProxy(`${type}/${id}`);
    const d = await res.json();
    const title = d.title || d.name || '';
    const cover = d.poster_path ? `https://image.tmdb.org/t/p/w500${d.poster_path}` : '';
    const genres = (d.genres || []).map(g => g.name).join(', ');
    const year = (d.release_date || d.first_air_date || '').slice(0, 4);

    const genreNames = (d.genres || []).map(g => g.name).filter(Boolean);
    selectedTmdb = {
      title,
      cover,
      genre: genres,
      genreNames,
      year,
      tmdbId: String(id),
      originalTitle: d.original_name || d.original_title || '',
      originalLanguage: d.original_language || '',
      originCountries: Array.isArray(d.origin_country) ? d.origin_country : []
    };
    if (type === "tv") {
      selectedTmdb.mediaCategory = detectAnimeFromMetadata(selectedTmdb) ? 'anime' : 'shows';
      selectedTmdb.librarySection = selectedTmdb.mediaCategory;
      selectedTmdb.isAnime = selectedTmdb.mediaCategory === 'anime';
    } else {
      selectedTmdb.mediaCategory = 'movies';
      selectedTmdb.librarySection = 'movies';
      selectedTmdb.isAnime = false;
    }

    // Get IMDb ID
    if (type === "movie" && d.imdb_id) {
      selectedTmdb.imdbId = d.imdb_id;
    } else if (type === "tv") {
      try {
        const extRes = await fetchTmdbProxy(`tv/${id}/external_ids`);
        const extData = await extRes.json();
        if (extData.imdb_id) selectedTmdb.imdbId = extData.imdb_id;
      } catch(e) {}
    }

    if (type === "tv") {
      // Fetch all episodes across all seasons
      const seasons = (d.seasons || []).filter(s => s.season_number > 0);
      let allEpisodes = [];
      for (const season of seasons) {
        try {
          const sRes = await fetchTmdbProxy(`tv/${id}/season/${season.season_number}`);
          const sData = await sRes.json();
          (sData.episodes || []).forEach(ep => {
            allEpisodes.push({
              number: allEpisodes.length + 1,
              seasonNum: season.season_number,
              epNum: ep.episode_number,
              title: ep.name || '',
            });
          });
        } catch(e) {}
      }
      selectedTmdb.episodes = allEpisodes;
      selectedTmdb.totalEpisodes = allEpisodes.length;
      selectedTmdb.seasons = seasons.length;
    }

    // Show selected preview
    resultsDiv.innerHTML = '';
    const selectedArea = document.getElementById("tmdb-selected-area");
    const coverThumb = d.poster_path ? `https://image.tmdb.org/t/p/w185${d.poster_path}` : '';
    const epInfo = type === "tv" ? `<div class="tmdb-selected-detail">${selectedTmdb.seasons} season${selectedTmdb.seasons > 1 ? 's' : ''} · ${selectedTmdb.totalEpisodes} episodes</div>` : '';
    selectedArea.style.display = "block";
    selectedArea.innerHTML = `<div class="tmdb-selected">
      ${coverThumb ? `<img src="${coverThumb}">` : ''}
      <div class="tmdb-selected-info">
        <div class="tmdb-selected-title">${escHtml(title)} ${year ? '(' + year + ')' : ''}</div>
        <div class="tmdb-selected-detail">${escHtml(genres)}</div>
        ${epInfo}
        <button class="tmdb-clear" onclick="clearSelection()">Clear selection</button>
      </div>
    </div>`;
    showModalStatusPicker();
    document.getElementById("tmdb-search-area").style.display = "none";
  } catch(e) {
    resultsDiv.innerHTML = '<div class="cover-search-msg">Failed to load details. Try again.</div>';
  }
}

function clearSelection() {
  selectedTmdb = null;
  document.getElementById("tmdb-selected-area").style.display = "none";
  document.getElementById("tmdb-selected-area").innerHTML = "";
  hideModalStatusPicker();
  document.getElementById("tmdb-search-area").style.display = "block";
}

const MODAL_STATUS_OPTIONS = {
  shows: [
    { status: 'watching', label: 'Watching' },
    { status: 'planned',  label: 'Plan to Watch' },
    { status: 'watched',  label: 'Watched' },
    { status: 'paused',   label: 'Paused' },
    { status: 'dropped',  label: 'Dropped' }
  ],
  anime: [
    { status: 'watching', label: 'Watching' },
    { status: 'planned',  label: 'Plan to Watch' },
    { status: 'watched',  label: 'Watched' },
    { status: 'paused',   label: 'Paused' },
    { status: 'dropped',  label: 'Dropped' }
  ],
  movies: [
    { status: 'planned',  label: 'Plan to Watch' },
    { status: 'watched',  label: 'Watched' },
    { status: 'paused',   label: 'Paused' },
    { status: 'dropped',  label: 'Dropped' }
  ],
  games: [
    { status: 'live',     label: 'Live Games' },
    { status: 'watching', label: 'Playing' },
    { status: 'planned',  label: 'Backlog' },
    { status: 'watched',  label: 'Completed' },
    { status: 'paused',   label: 'Paused' },
    { status: 'dropped',  label: 'Dropped' }
  ]
};

function showModalStatusPicker() {
  const picker = document.getElementById("modal-status-picker");
  if (!picker) return;
  const options = MODAL_STATUS_OPTIONS[activeSection] || MODAL_STATUS_OPTIONS.shows;
  picker.innerHTML = `
    <div class="modal-status-label">Where do you want it?</div>
    <div class="modal-status-grid">
      ${options.map(o => `<button class="modal-status-btn" onclick="submitModal('${o.status}')">${escHtml(o.label)}</button>`).join('')}
    </div>
  `;
  picker.style.display = "flex";
}

function hideModalStatusPicker() {
  const picker = document.getElementById("modal-status-picker");
  if (!picker) return;
  picker.style.display = "none";
  picker.innerHTML = "";
}

// Modal
function openModal() {
  document.getElementById("modal").style.display = "flex";
  document.getElementById("modal-title").textContent = `Add ${activeSection === 'anime' ? 'Anime' : activeSection === 'shows' ? 'TV Show' : activeSection === 'movies' ? 'Movie' : 'Game'}`;
  document.getElementById("inp-tmdb-search").value = "";
  document.getElementById("inp-tmdb-search").placeholder = activeSection === 'games' ? 'Search RAWG...' : 'Search TMDB...';
  document.getElementById("tmdb-results").innerHTML = "";
  document.getElementById("tmdb-search-area").style.display = "block";
  clearSelection();
  renderApiKeySection();
  setTimeout(() => document.getElementById("inp-tmdb-search").focus(), 100);
}
function closeModal() {
  document.getElementById("modal").style.display = "none";
}
function isDuplicateTitle(title, section, excludeId = null) {
  const normalized = (title || '').trim().toLowerCase();
  if (!normalized) return false;
  return (data[section] || []).some(item =>
    item &&
    item.id !== excludeId &&
    (item.title || '').trim().toLowerCase() === normalized
  );
}


function isDuplicateTitleInList(title, section, sourceData, excludeId = null) {
  const normalized = (title || '').trim().toLowerCase();
  if (!normalized || !sourceData || !Array.isArray(sourceData[section])) return false;
  return sourceData[section].some(item =>
    item &&
    item.id !== excludeId &&
    (item.title || '').trim().toLowerCase() === normalized
  );
}

function sanitizeFriendCopy(source, section, status = 'planned', rating = 0) {
  const item = JSON.parse(JSON.stringify(source || {}));
  const newId = Date.now().toString() + '-friend-' + Math.random().toString(36).slice(2, 7);
  item.id = newId;
  item.status = status;
  item.rating = rating;
  item.dateAdded = new Date().toISOString();
  if (isShowSection(section)) {
    item.seasonRatings = {};
    item.episodes = (item.episodes || []).map((ep, i) => ({
      ...ep,
      id: newId + '-ep-' + (i + 1),
      watched: status === 'watched',
      rating: 0,
    }));
    item.totalEpisodes = item.episodes.length;
    item.totalEps = item.episodes.length;
    item.currentEp = status === 'watched' ? item.episodes.length : 0;
  }
  return item;
}

let pendingFriendAdd = null;

function openFriendAddModal(itemId, btn) {
  if (!viewingUser || !currentUser || !myData || !friendViewData) return;
  const section = activeSection;
  const source = (friendViewData[section] || []).find(item => item.id === itemId);
  if (!source) return;

  if (isDuplicateTitleInList(source.title, section, myData)) {
    showToast("this title is already added to your library silly!");
    if (btn) {
      btn.textContent = '✓';
      btn.classList.add('added');
    }
    return;
  }

  pendingFriendAdd = { itemId, btn };
  renderFriendAddChoice();
  document.getElementById('discover-add-modal').style.display = 'flex';
}

function renderFriendAddChoice() {
  const content = document.getElementById('discover-add-modal-content');
  if (!content || !pendingFriendAdd) return;
  const isGame = activeSection === 'games';
  const watchedLabel = isGame ? 'Completed' : 'Watched';
  const plannedLabel = isGame ? 'Backlog' : 'Planned to Watch';
  content.innerHTML = `
    <h3>Add to Library</h3>
    <div class="discover-add-desc">Where you bouta put this?</div>
    <div class="discover-status-options">
      <button class="discover-status-btn watched-option" onclick="confirmFriendAdd('watched')">${watchedLabel}</button>
      <button class="discover-status-btn planned-option" onclick="confirmFriendAdd('planned')">${plannedLabel}</button>
    </div>
    <div class="modal-actions">
      <button class="btn-secondary discover-cancel-btn" onclick="closeDiscoverAddModal()">Cancel</button>
    </div>
  `;
}

function confirmFriendAdd(status) {
  if (!pendingFriendAdd) return;
  if (status === 'watched') {
    renderFriendRatingPrompt(0);
    return;
  }
  finalizeFriendAdd(status, 0);
}

function renderFriendRatingPrompt(selectedRating = 0) {
  const content = document.getElementById('discover-add-modal-content');
  if (!content || !pendingFriendAdd) return;
  const skipLabel = activeSection === 'games' ? 'completed' : 'watched';
  let stars = '';
  for (let i = 1; i <= 10; i++) {
    stars += `<button class="star-btn ${i <= selectedRating ? 'lit' : ''}"
      onclick="selectFriendRating(${i})"
      onmouseenter="hoverStars(this,${i})"
      onmouseleave="unhoverStars(this,${selectedRating})">★</button>`;
  }
  content.innerHTML = `
    <div class="discover-rating-prompt">
      <h3>Rate this Title</h3>
      <div class="discover-add-desc">Choose a rating, or skip and add it as ${skipLabel}.</div>
      <div class="stars discover-rating-stars" data-discover-rating="${selectedRating}">${stars}${selectedRating > 0 ? `<span class="star-label">${selectedRating}</span>` : ''}</div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="renderFriendAddChoice()">Back</button>
        <button class="btn-secondary" onclick="finalizeFriendAdd('watched', 0)">Skip</button>
      </div>
    </div>
  `;
}

function selectFriendRating(score) {
  if (!pendingFriendAdd || pendingFriendAdd.ratingLock) return;
  pendingFriendAdd.ratingLock = true;
  const container = document.querySelector('#discover-add-modal .discover-rating-stars');
  if (container) {
    container.dataset.discoverRating = score;
    container.querySelectorAll('.star-btn').forEach((star, index) => {
      const lit = index + 1 <= score;
      star.classList.toggle('lit', lit);
      star.style.color = lit ? '#f59e0b' : '#443d60';
      star.style.transform = 'scale(1)';
    });
    let label = container.querySelector('.star-label');
    if (!label) {
      label = document.createElement('span');
      label.className = 'star-label';
      container.appendChild(label);
    }
    label.textContent = score;
    const animationMs = playDiscoveryModalRatingAnimation(score, container);
    setTimeout(() => finalizeFriendAdd('watched', score), animationMs);
    return;
  }
  finalizeFriendAdd('watched', score);
}

function finalizeFriendAdd(status, rating = 0) {
  if (!pendingFriendAdd) return;
  const pending = pendingFriendAdd;
  document.getElementById('discover-add-modal').style.display = 'none';
  pendingFriendAdd = null;
  addFriendTitleToMyList(pending.itemId, pending.btn, status, rating);
}

async function addFriendTitleToMyList(itemId, btn, status = 'planned', rating = 0) {
  if (!viewingUser || !currentUser || !friendViewData) return;
  const section = activeSection;
  const source = (friendViewData[section] || []).find(item => item.id === itemId);
  if (!source) return;

  const targetData = myData ? cloneListData(myData) : (ownDataCache ? cloneListData(ownDataCache) : await loadOwnDataFromFirestore());
  if (isDuplicateTitleInList(source.title, section, targetData)) {
    showToast("this title is already added to your library silly!");
    if (btn) {
      btn.textContent = '✓';
      btn.classList.add('added');
    }
    return;
  }

  const item = sanitizeFriendCopy(source, section, status, rating);
  targetData[section] = Array.isArray(targetData[section]) ? targetData[section] : [];
  targetData[section].push(item);

  if (btn) {
    btn.textContent = '✓';
    btn.classList.add('added');
  }

  try {
    await writeOwnDataDirect(targetData);
    myData = cloneListData(targetData);
    showToast("Added to your library");
  } catch(e) {
    console.error("Friend profile add failed:", e);
    if (btn) {
      btn.textContent = '+';
      btn.classList.remove('added');
    }
    showToast("Could not add this title. Try again.");
  }
}

function submitModal(status) {
  if (!selectedTmdb) return;
  const validStatuses = (MODAL_STATUS_OPTIONS[activeSection] || []).map(o => o.status);
  if (!validStatuses.includes(status)) status = activeSection === 'movies' ? 'planned' : 'watching';
  const targetSection = isShowSection(activeSection)
    ? resolveShowSection(selectedTmdb, activeSection)
    : activeSection;
  if (isDuplicateTitle(selectedTmdb.title, targetSection)) {
    showToast("this title is already added to your library silly!");
    return;
  }
  const item = {
    id: Date.now().toString(),
    title: selectedTmdb.title,
    cover: selectedTmdb.cover,
    genre: selectedTmdb.genre,
    year: selectedTmdb.year || '',
    status,
    rating: 0,
    dateAdded: new Date().toISOString(),
    imdbId: selectedTmdb.imdbId || '',
    platforms: selectedTmdb.platforms || '',
    metacriticSlug: selectedTmdb.metacriticSlug || '',
    tmdbId: selectedTmdb.tmdbId || '',
    mediaCategory: selectedTmdb.mediaCategory || (isShowSection(activeSection) ? resolveShowSection(selectedTmdb, activeSection) : activeSection),
    librarySection: selectedTmdb.mediaCategory || (isShowSection(activeSection) ? resolveShowSection(selectedTmdb, activeSection) : activeSection),
    originalTitle: selectedTmdb.originalTitle || '',
    originalLanguage: selectedTmdb.originalLanguage || '',
    originCountries: Array.isArray(selectedTmdb.originCountries) ? selectedTmdb.originCountries : [],
    genreNames: Array.isArray(selectedTmdb.genreNames) ? selectedTmdb.genreNames : [],
    isAnime: (selectedTmdb.mediaCategory || '') === 'anime',
  };
  if (isShowSection(activeSection) && selectedTmdb.episodes) {
    item.totalEpisodes = selectedTmdb.totalEpisodes;
    item.episodes = selectedTmdb.episodes.map((ep, i) => ({
      id: item.id + '-ep-' + (i + 1),
      number: ep.number,
      seasonNum: ep.seasonNum,
      epNum: ep.epNum,
      title: ep.title,
      watched: status === 'watched',
      rating: 0,
    }));
  }
  data[targetSection].push(item);
  save();
  closeModal();
  activeSection = targetSection;
  activeTab = status;
  render();
}


// Discovery
let discoverLoaded = false;
let discoverLoading = false;
let gamesDiscoverLoaded = false;
let gamesDiscoverLoading = false;
const DISCOVER_PAGE_COUNT = 5;
const DISCOVER_LIMIT = 40;
const DISCOVER_STREAMING_REGION = 'US';

function getDiscoverGrids() {
  return [
    document.getElementById('discover-movies-grid'),
    document.getElementById('discover-tv-grid'),
    document.getElementById('discover-streaming-grid')
  ].filter(Boolean);
}

function getGamesDiscoverGrids() {
  return [
    document.getElementById('discover-games-popular-grid'),
    document.getElementById('discover-games-trending-grid'),
    document.getElementById('discover-games-new-releases-grid'),
    document.getElementById('discover-games-anticipated-grid'),
    document.getElementById('discover-games-rated-grid'),
    document.getElementById('discover-games-story-grid'),
    document.getElementById('discover-games-multiplayer-grid'),
    document.getElementById('discover-games-hidden-grid')
  ].filter(Boolean);
}

function getAllDiscoverGrids() {
  return getDiscoverGrids().concat(getGamesDiscoverGrids());
}

function getDiscoverExpandButton(grid) {
  return grid ? document.querySelector(`.discover-expand-btn[data-discover-target="${grid.id}"]`) : null;
}

function hideDiscoverExpandButtons() {
  document.querySelectorAll('.discover-expand-btn').forEach(btn => btn.style.display = 'none');
}

function renderDiscoverLoading() {
  hideDiscoverExpandButtons();
  const loading = '<div class="discover-message">Loading discovery titles...</div>';
  getDiscoverGrids().forEach(grid => grid.innerHTML = loading);
  const activityFeed = document.getElementById('friend-activity-feed');
  if (activityFeed) activityFeed.innerHTML = '<div class="discover-message">Loading friend activity...</div>';
}

function renderDiscoverError(message) {
  hideDiscoverExpandButtons();
  const html = `<div class="discover-message">${escHtml(message)}</div>`;
  getDiscoverGrids().forEach(grid => grid.innerHTML = html);
}

function relativeTime(iso) {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${Math.max(1, mins)}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 7) return `${days}d ago`;
  return `${Math.floor(days / 7)}w ago`;
}

function getActivityAction(item) {
  if (item.rating > 0) return { verb: 'rated', extra: ` ${item.rating}/10 &#9733;`, isRating: true };
  if (item.status === 'watching') return { verb: 'is watching', extra: '', isRating: false };
  if (item.status === 'watched') return { verb: 'watched', extra: '', isRating: false };
  if (item.status === 'planned') return { verb: 'wants to watch', extra: '', isRating: false };
  return { verb: 'added', extra: '', isRating: false };
}

function renderPreviewFriendActivity() {
  const feed = document.getElementById('friend-activity-feed');
  if (!feed) return;
  PREVIEW_COMMUNITY_USERS.forEach(user => { usersMap[user.uid] = user; });
  const demo = PREVIEW_COMMUNITY_USERS.map((user, index) => {
    const sections = ['shows', 'movies', 'anime', 'games'];
    const item = sections.flatMap(section => user.listData[section] || []).find(entry => entry.title);
    return item ? { uid: user.uid, name: user.name, photo: user.photo, item: { ...item, dateAdded: new Date(Date.now() - (index + 1) * 45 * 60000).toISOString() } } : null;
  }).filter(Boolean);
  if (!demo.length) {
    feed.innerHTML = '<div class="discover-message">Preview activity appears here with demo profiles.</div>';
    return;
  }
  renderFriendActivityItems(feed, demo);
}

let friendActivityClickTargets = {};

function buildActivityItemHTML(a, activityId) {
  const item = a.item || {};
  const actor = usersMap[a.uid] ? { ...usersMap[a.uid], ...a } : a;
  const { verb, extra, isRating } = a.type === 'comment'
    ? { verb: 'commented on', extra: '', isRating: false }
    : getActivityAction(item);
  const avatarHtml = a.photo
    ? `<img class="activity-avatar" src="${escAttr(a.photo)}" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
    : '';
  const placeholderStyle = a.photo ? 'display:none' : '';
  const coverHtml = item.cover
    ? `<img class="activity-cover" src="${escAttr(item.cover)}" alt="" loading="lazy">`
    : `<div class="activity-cover-placeholder"></div>`;
  const extraHtml = extra
    ? `<span class="${isRating ? 'activity-rating' : 'activity-action'}">${extra}</span>`
    : '';
  return `<div class="activity-item" onclick="handleFriendActivityClick('${escAttr(activityId)}')">
    ${avatarHtml}<div class="activity-avatar-placeholder" style="${placeholderStyle}">&#128100;</div>
    <div class="activity-text">
      <div>${renderDisplayNameHTML(actor, 'Friend', 'activity-name')} <span class="activity-action">${verb}</span> <span class="activity-title">${escHtml(item.title || 'Untitled')}</span>${extraHtml}</div>
      <div class="activity-time">${relativeTime(a.timestamp || item.dateAdded)}</div>
    </div>
    ${coverHtml}
  </div>`;
}

function renderFriendActivityItems(feed, activities) {
  friendActivityClickTargets = {};
  feed.innerHTML = `<div class="activity-feed">${activities.map((activity, index) => {
    const id = `activity-${Date.now()}-${index}`;
    friendActivityClickTargets[id] = activity;
    return buildActivityItemHTML(activity, id);
  }).join('')}</div>`;
}

function handleFriendActivityClick(activityId) {
  const activity = friendActivityClickTargets[activityId];
  if (!activity) return;
  if (activity.type === 'comment') {
    const item = activity.item || {};
    openCommentsPageForActivity(activity.mediaKey, item.title || 'Untitled', item.cover || '', activity.commentId || '');
    return;
  }
  viewUserFromMap(activity.uid);
}

async function fetchAllFriendActivities(dayLimit = 7) {
  if (isPreviewMode()) {
    return PREVIEW_COMMUNITY_USERS.map((user, index) => {
      const sections = ['shows', 'movies', 'anime', 'games'];
      const item = sections.flatMap(section => user.listData[section] || []).find(entry => entry.title);
      return item ? { uid: user.uid, name: user.name, photo: user.photo, item: { ...item, dateAdded: new Date(Date.now() - (index + 1) * 45 * 60000).toISOString() } } : null;
    }).filter(Boolean);
  }
  if (!currentUser || !friends.length) return [];
  const cutoff = dayLimit ? new Date(Date.now() - dayLimit * 24 * 60 * 60 * 1000).toISOString() : null;
  const activities = [];
  const mediaMap = new Map();
  const friendUidSet = new Set(friends);
  await Promise.all(friends.map(async uid => {
    try {
      if (!usersMap[uid]) {
        const userSnap = await db.collection('users').doc(uid).get();
        if (userSnap.exists) usersMap[uid] = { ...userSnap.data(), uid };
      }
      const snap = await db.collection('watchlist').doc(uid).get();
      if (!snap.exists) return;
      const d = snap.data();
      const u = usersMap[uid] || {};
      for (const section of ['movies', 'shows', 'anime', 'games']) {
        let items = [];
        try { items = d[section] ? JSON.parse(d[section]) : []; } catch(e) {}
        for (const item of items) {
          const mediaKey = getMediaKey({ ...item, librarySection: section, mediaCategory: section });
          if (mediaKey && !mediaMap.has(mediaKey)) mediaMap.set(mediaKey, { title: item.title, cover: item.cover || '', section });
          if (!item.dateAdded) continue;
          if (cutoff && item.dateAdded < cutoff) continue;
          activities.push({ uid, name: u.name || 'Friend', photo: u.photo || '', item, timestamp: item.dateAdded });
        }
      }
    } catch(e) {}
  }));
  await Promise.all(Array.from(mediaMap.entries()).map(async ([mediaKey, media]) => {
    try {
      const snap = await db.collection('comments').doc(mediaKey).get();
      if (!snap.exists) return;
      const comments = Array.isArray(snap.data().comments) ? snap.data().comments : [];
      comments.forEach(comment => {
        if (!friendUidSet.has(comment.uid)) return;
        const commentIso = comment.timestamp ? new Date(comment.timestamp).toISOString() : '';
        if (cutoff && commentIso && commentIso < cutoff) return;
        activities.push({
          type: 'comment',
          uid: comment.uid,
          name: comment.name || usersMap[comment.uid]?.name || 'Friend',
          photo: comment.photo || usersMap[comment.uid]?.photo || '',
          item: { title: media.title, cover: media.cover, dateAdded: commentIso },
          mediaKey,
          commentId: comment.id,
          timestamp: comment.timestamp || Date.now()
        });
      });
    } catch(e) {}
  }));
  activities.sort((a, b) => new Date(b.timestamp || b.item.dateAdded) - new Date(a.timestamp || a.item.dateAdded));
  return activities;
}

async function loadFriendActivity() {
  const feed = document.getElementById('friend-activity-feed');
  if (!feed) return;
  if (isPreviewMode()) {
    renderPreviewFriendActivity();
    return;
  }
  if (!currentUser || !friends.length) {
    feed.innerHTML = '<div class="discover-message">Add some friends to see their activity here.</div>';
    return;
  }
  feed.innerHTML = '<div class="discover-message">Loading friend activity...</div>';
  const activities = await fetchAllFriendActivities(7);
  if (!activities.length) {
    feed.innerHTML = '<div class="discover-message">No friend activity in the last 7 days.</div>';
    return;
  }
  renderFriendActivityItems(feed, activities.slice(0, 6));
}

function openActivityPage() {
  const page = document.getElementById('activity-page');
  const discoverView = document.getElementById('discover-view');
  const mainNav = document.querySelector('.main-nav');
  if (!page) return;
  if (discoverView) discoverView.style.display = 'none';
  if (mainNav) mainNav.style.display = 'none';
  page.classList.add('active');
  loadFullActivityFeed();
  persistUiState();
}

function closeActivityPage() {
  const page = document.getElementById('activity-page');
  const discoverView = document.getElementById('discover-view');
  const mainNav = document.querySelector('.main-nav');
  if (page) page.classList.remove('active');
  if (discoverView) discoverView.style.display = 'block';
  if (mainNav) mainNav.style.display = 'flex';
  persistUiState();
}

async function loadFullActivityFeed() {
  const feed = document.getElementById('activity-page-feed');
  if (!feed) return;
  if (isPreviewMode()) {
    renderFriendActivityItems(feed, await fetchAllFriendActivities(0));
    return;
  }
  if (!currentUser || !friends.length) {
    feed.innerHTML = '<div class="discover-message">Add some friends to see their activity here.</div>';
    return;
  }
  feed.innerHTML = '<div class="discover-message">Loading activity...</div>';
  const activities = await fetchAllFriendActivities(0);
  if (!activities.length) {
    feed.innerHTML = '<div class="discover-message">No friend activity yet.</div>';
    return;
  }
  renderFriendActivityItems(feed, activities);
}


function renderGamesDiscoverLoading() {
  getGamesDiscoverGrids().forEach(grid => {
    const button = getDiscoverExpandButton(grid);
    if (button) button.style.display = 'none';
    grid.innerHTML = '<div class="discover-message">Loading game discovery titles...</div>';
  });
}

function renderGamesDiscoverError(message) {
  const html = `<div class="discover-message">${escHtml(message)}</div>`;
  getGamesDiscoverGrids().forEach(grid => {
    const button = getDiscoverExpandButton(grid);
    if (button) button.style.display = 'none';
    grid.innerHTML = html;
  });
}

async function loadDiscover(force = false) {
  if (discoverLoading) return;
  if (discoverLoaded && !force) {
    loadFriendActivity();
    return;
  }
  discoverLoading = true;
  renderDiscoverLoading();
  try {
    const [movies, tv, streaming] = await Promise.all([
      fetchDiscoverTitles('movie'),
      fetchDiscoverTitles('tv'),
      fetchStreamingTitles()
    ]);
    renderDiscoverCards('movie', movies, 'discover-movies-grid');
    renderDiscoverCards('tv', tv, 'discover-tv-grid');
    renderDiscoverCards('mixed', streaming, 'discover-streaming-grid');
    discoverLoaded = true;
  } catch(e) {
    console.error("Discover load failed:", e);
    renderDiscoverError("Discover could not load. Try refreshing.");
  } finally {
    discoverLoading = false;
  }
  loadFriendActivity();
}

async function fetchTmdbPages(path, params = {}, pageCount = DISCOVER_PAGE_COUNT) {
  const pages = Array.from({ length: pageCount }, (_, i) => i + 1);
  const results = await Promise.all(pages.map(async page => {
    const res = await fetchTmdbProxy(path, { ...params, page: String(page) });
    if (!res.ok) throw new Error("TMDB discover request failed");
    const json = await res.json();
    return json.results || [];
  }));
  const seen = new Set();
  return results.flat().filter(item => {
    const key = `${item.media_type || path}:${item.id}`;
    if (!item.poster_path || seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, DISCOVER_LIMIT);
}

async function fetchDiscoverTitles(type) {
  return fetchTmdbPages(`trending/${type}/week`);
}

async function fetchStreamingTitles() {
  const [movies, tv] = await Promise.all([
    fetchTmdbPages('discover/movie', {
      sort_by: 'popularity.desc',
      watch_region: DISCOVER_STREAMING_REGION,
      with_watch_monetization_types: 'flatrate'
    }, DISCOVER_PAGE_COUNT),
    fetchTmdbPages('discover/tv', {
      sort_by: 'popularity.desc',
      watch_region: DISCOVER_STREAMING_REGION,
      with_watch_monetization_types: 'flatrate'
    }, DISCOVER_PAGE_COUNT)
  ]);
  return movies.map(item => ({ ...item, media_type: 'movie' }))
    .concat(tv.map(item => ({ ...item, media_type: 'tv' })))
    .sort((a, b) => (b.popularity || 0) - (a.popularity || 0))
    .slice(0, DISCOVER_LIMIT);
}

function handleDiscoverSearchKey(event, source) {
  if (event.key !== 'Enter') return;
  event.preventDefault();
  if (source === 'rawg') searchGamesDiscoverDatabase();
  else searchDiscoverDatabase();
}

function setDiscoverSearchSection(source, visible) {
  const id = source === 'rawg' ? 'games-discover-search-section' : 'discover-search-section';
  const section = document.getElementById(id);
  if (!section) return;
  section.classList.toggle('active', visible);
  section.style.display = visible ? 'block' : 'none';
}

function clearDiscoverDatabaseSearch(source) {
  const isRawg = source === 'rawg';
  const input = document.getElementById(isRawg ? 'games-discover-search-input' : 'discover-search-input');
  const grid = document.getElementById(isRawg ? 'discover-games-search-grid' : 'discover-search-grid');
  const button = grid ? getDiscoverExpandButton(grid) : null;
  if (input) input.value = '';
  if (grid) {
    grid.innerHTML = '';
    grid.dataset.expanded = 'false';
  }
  if (button) button.style.display = 'none';
  setDiscoverSearchSection(source, false);
}

async function fetchTmdbSearchResults(query) {
  const pages = Array.from({ length: DISCOVER_PAGE_COUNT }, (_, i) => i + 1);
  const settled = await Promise.allSettled(pages.map(async page => {
    const res = await fetchTmdbProxy('search/multi', { query, page: String(page) });
    if (!res.ok) throw new Error(`TMDB search request failed: ${res.status}`);
    const json = await res.json();
    return json.results || [];
  }));
  const seen = new Set();
  return settled
    .filter(result => result.status === 'fulfilled')
    .flatMap(result => result.value)
    .filter(item => {
      const type = item.media_type;
      const key = `${type}:${item.id}`;
      if ((type !== 'movie' && type !== 'tv') || !item.poster_path || seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => {
      const scoreA = (Number(a.popularity || 0) * 1.4) + (Number(a.vote_count || 0) * 0.06) + (Number(a.vote_average || 0) * 5);
      const scoreB = (Number(b.popularity || 0) * 1.4) + (Number(b.vote_count || 0) * 0.06) + (Number(b.vote_average || 0) * 5);
      return scoreB - scoreA;
    })
    .slice(0, DISCOVER_LIMIT);
}

async function searchDiscoverDatabase() {
  const input = document.getElementById('discover-search-input');
  const grid = document.getElementById('discover-search-grid');
  if (!input || !grid) return;
  const query = input.value.trim();
  if (!query) return clearDiscoverDatabaseSearch('tmdb');
  setDiscoverSearchSection('tmdb', true);
  const button = getDiscoverExpandButton(grid);
  if (button) button.style.display = 'none';
  grid.innerHTML = '<div class="discover-message">Searching TMDB...</div>';
  try {
    const items = await fetchTmdbSearchResults(query);
    renderDiscoverCards('mixed', items, 'discover-search-grid');
  } catch(e) {
    console.error('TMDB search failed:', e);
    grid.innerHTML = '<div class="discover-message">Search failed. Try again.</div>';
  }
}

async function fetchRawgSearchResults(query) {
  const pool = await fetchRawgPages({ search: query, ordering: '-added' }, DISCOVER_PAGE_COUNT, DISCOVER_LIMIT);
  return rankGames(pool, 'popular');
}

async function searchGamesDiscoverDatabase() {
  const input = document.getElementById('games-discover-search-input');
  const grid = document.getElementById('discover-games-search-grid');
  if (!input || !grid) return;
  const query = input.value.trim();
  if (!query) return clearDiscoverDatabaseSearch('rawg');
  setDiscoverSearchSection('rawg', true);
  const button = getDiscoverExpandButton(grid);
  if (button) button.style.display = 'none';
  grid.innerHTML = '<div class="discover-message">Searching RAWG...</div>';
  try {
    const items = await fetchRawgSearchResults(query);
    renderGamesDiscoverCards(items, 'discover-games-search-grid');
  } catch(e) {
    console.error('RAWG search failed:', e);
    grid.innerHTML = '<div class="discover-message">Search failed. Try again.</div>';
  }
}


function getRawgDateString(date) {
  return date.toISOString().slice(0, 10);
}

async function fetchRawgPages(params = {}, pageCount = DISCOVER_PAGE_COUNT, limit = DISCOVER_LIMIT) {
  const pages = Array.from({ length: pageCount }, (_, i) => i + 1);
  const settled = await Promise.allSettled(pages.map(async page => {
    const res = await fetchRawgProxy('games', { page_size: '40', ...params, page: String(page) });
    if (!res.ok) throw new Error(`RAWG discovery request failed: ${res.status}`);
    const json = await res.json();
    return json.results || [];
  }));
  const results = settled
    .filter(result => result.status === 'fulfilled')
    .flatMap(result => result.value);
  const seen = new Set();
  return results.filter(item => {
    if (!item || !item.id || !item.name || !item.background_image || seen.has(item.id)) return false;
    seen.add(item.id);
    return true;
  }).slice(0, limit);
}

function gameRatingCount(item) {
  return Number(item.ratings_count || item.reviews_count || 0);
}

function gameAddedCount(item) {
  return Number(item.added || 0);
}

function gameMajorPlatformScore(item) {
  const major = ['pc', 'playstation', 'xbox', 'nintendo'];
  const names = (item.platforms || []).map(p => (p.platform?.name || '').toLowerCase());
  return names.filter(name => major.some(platform => name.includes(platform))).length;
}

function rankGames(items, kind) {
  const scored = items.map(item => {
    const rating = Number(item.rating || 0);
    const ratingCount = gameRatingCount(item);
    const added = gameAddedCount(item);
    const metacritic = Number(item.metacritic || 0);
    let score = 0;

    if (kind === 'popular') score = added * 1.1 + ratingCount * 18 + rating * 220 + metacritic * 4;
    else if (kind === 'trending') score = added * 1.2 + ratingCount * 14 + rating * 260 + metacritic * 3;
    else if (kind === 'new-releases') score = added * 1.35 + ratingCount * 18 + rating * 240 + metacritic * 3 + gameMajorPlatformScore(item) * 100;
    else if (kind === 'anticipated') score = added * 1.45 + ratingCount * 24 + rating * 180 + metacritic * 3 + gameMajorPlatformScore(item) * 120;
    else if (kind === 'rated') score = rating * 850 + metacritic * 8 + Math.min(ratingCount, 2500) * 2 + Math.min(added, 20000) * 0.02;
    else if (kind === 'story') score = rating * 520 + metacritic * 5 + ratingCount * 8 + added * 0.18;
    else if (kind === 'multiplayer') score = added * 0.85 + ratingCount * 10 + rating * 260 + metacritic * 2;
    else if (kind === 'hidden') score = rating * 780 + metacritic * 4 + Math.min(ratingCount, 1200) * 4 - Math.max(0, added - 12000) * 0.08;

    return { item, score };
  });

  return scored.sort((a, b) => b.score - a.score).map(entry => entry.item).slice(0, DISCOVER_LIMIT);
}

async function fetchGamesDiscoverTitles(kind) {
  if (kind === 'trending') {
    const today = new Date();
    const past = new Date();
    past.setMonth(today.getMonth() - 18);
    let pool = await fetchRawgPages({
      dates: `${getRawgDateString(past)},${getRawgDateString(today)}`,
      ordering: '-added'
    }, DISCOVER_PAGE_COUNT, 120);
    let ranked = rankGames(pool.filter(item => Number(item.rating || 0) >= 3.4 || gameRatingCount(item) >= 75), 'trending');
    if (ranked.length) return ranked;
    pool = await fetchRawgPages({ dates: `${getRawgDateString(past)},${getRawgDateString(today)}`, ordering: '-metacritic' }, 3, 80);
    return rankGames(pool, 'trending');
  }
  if (kind === 'new-releases') {
    const today = new Date();
    const start = new Date(today.getFullYear(), today.getMonth(), 1);
    const end = new Date(today.getFullYear(), today.getMonth() + 1, 0);
    const startString = getRawgDateString(start);
    const endString = getRawgDateString(end);
    let pool = await fetchRawgPages({
      dates: `${startString},${endString}`,
      ordering: '-added'
    }, DISCOVER_PAGE_COUNT, 160);
    let ranked = rankGames(pool.filter(item => {
      if (!item.released) return false;
      const releaseDate = new Date(`${item.released}T00:00:00`);
      return releaseDate >= new Date(`${startString}T00:00:00`) && releaseDate <= new Date(`${endString}T23:59:59`);
    }), 'new-releases');
    if (ranked.length) return ranked;
    pool = await fetchRawgPages({ dates: `${startString},${endString}`, ordering: '-released' }, 3, 100);
    return rankGames(pool, 'new-releases');
  }
  if (kind === 'anticipated') {
    const today = new Date();
    const future = new Date();
    future.setFullYear(today.getFullYear() + 2);
    const todayString = getRawgDateString(today);
    const futureString = getRawgDateString(future);
    const pool = await fetchRawgPages({
      dates: `${todayString},${futureString}`,
      ordering: '-added'
    }, DISCOVER_PAGE_COUNT, 180);
    return rankGames(pool.filter(item => {
      if (!item.released) return false;
      const releaseDate = new Date(`${item.released}T00:00:00`);
      return releaseDate >= new Date(`${todayString}T00:00:00`) && releaseDate <= new Date(`${futureString}T23:59:59`);
    }), 'anticipated');
  }
  if (kind === 'rated') {
    let pool = await fetchRawgPages({ ordering: '-rating', metacritic: '75,100' }, DISCOVER_PAGE_COUNT, 160);
    let ranked = rankGames(pool.filter(item => Number(item.rating || 0) >= 4 && gameRatingCount(item) >= 75), 'rated');
    if (ranked.length) return ranked;
    pool = await fetchRawgPages({ ordering: '-metacritic' }, 3, 100);
    return rankGames(pool.filter(item => gameRatingCount(item) >= 25 || Number(item.metacritic || 0) >= 75), 'rated');
  }
  if (kind === 'story') {
    let pool = await fetchRawgPages({
      ordering: '-added',
      genres: 'action,adventure,role-playing-games-rpg',
      tags: 'open-world,story-rich,singleplayer'
    }, DISCOVER_PAGE_COUNT, 160);
    let ranked = rankGames(pool.filter(item => Number(item.rating || 0) >= 3.5 || gameRatingCount(item) >= 100), 'story');
    if (ranked.length) return ranked;
    pool = await fetchRawgPages({ ordering: '-added', genres: 'adventure,role-playing-games-rpg' }, 3, 100);
    return rankGames(pool, 'story');
  }
  if (kind === 'multiplayer') {
    let pool = await fetchRawgPages({
      ordering: '-added',
      tags: 'multiplayer,co-op,online-co-op,pvp,competitive,party'
    }, DISCOVER_PAGE_COUNT, 160);
    let ranked = rankGames(pool.filter(item => gameRatingCount(item) >= 50 || gameAddedCount(item) >= 1000), 'multiplayer');
    if (ranked.length) return ranked;
    pool = await fetchRawgPages({ ordering: '-added', tags: 'multiplayer' }, 3, 100);
    return rankGames(pool, 'multiplayer');
  }
  if (kind === 'hidden') {
    const pool = await fetchRawgPages({ ordering: '-rating' }, DISCOVER_PAGE_COUNT, 200);
    const strict = rankGames(pool.filter(item => {
      const rating = Number(item.rating || 0);
      const ratingCount = gameRatingCount(item);
      const added = gameAddedCount(item);
      return rating >= 3.7 && ratingCount >= 35 && added <= 22000;
    }), 'hidden');
    if (strict.length) return strict;
    return rankGames(pool.filter(item => Number(item.rating || 0) >= 3.5 && gameAddedCount(item) <= 50000), 'hidden');
  }
  return rankGames(await fetchRawgPages({ ordering: '-added' }, DISCOVER_PAGE_COUNT, 120), 'popular');
}

function renderGamesDiscoverSectionError(gridId) {
  const grid = document.getElementById(gridId);
  if (!grid) return;
  const button = getDiscoverExpandButton(grid);
  if (button) button.style.display = 'none';
  grid.innerHTML = '<div class="discover-message">This section could not load. Other sections are still available.</div>';
}

const discoverTrailerCache = new Map();
let activeDiscoverPinnedCard = null;
let discoverCardPressTimer = null;
let discoverCardPressPoster = null;
let discoverCardPressStartX = 0;
let discoverCardPressStartY = 0;
const discoverCardLongPressMs = 560;
const discoverCardPressMoveThreshold = 12;
const DISCOVER_MOVIE_GENRE_MAP = {
  28: 'Action', 12: 'Adventure', 16: 'Animation', 35: 'Comedy', 80: 'Crime',
  99: 'Documentary', 18: 'Drama', 10751: 'Family', 14: 'Fantasy', 36: 'History',
  27: 'Horror', 10402: 'Music', 9648: 'Mystery', 10749: 'Romance', 878: 'Sci-Fi',
  10770: 'TV Movie', 53: 'Thriller', 10752: 'War', 37: 'Western'
};
const DISCOVER_TV_GENRE_MAP = {
  10759: 'Action', 16: 'Animation', 35: 'Comedy', 80: 'Crime', 99: 'Documentary',
  18: 'Drama', 10751: 'Family', 10762: 'Kids', 9648: 'Mystery', 10763: 'News',
  10764: 'Reality', 10765: 'Sci-Fi', 10766: 'Soap', 10767: 'Talk', 10768: 'War'
};

function getDiscoverTrailerCacheKey(type, id) {
  return `${type}-${id}`;
}

function scoreDiscoverTrailer(video) {
  const kind = String(video?.type || '').toLowerCase();
  if (video?.site !== 'YouTube' || !video?.key) return -1;
  if (video?.official && kind === 'trailer') return 400;
  if (kind === 'trailer') return 300;
  if (kind === 'teaser') return 200;
  return 100;
}

function pickBestDiscoverTrailer(videos) {
  const sorted = (videos || [])
    .filter(video => video?.site === 'YouTube' && video?.key)
    .sort((a, b) => {
      const scoreDiff = scoreDiscoverTrailer(b) - scoreDiscoverTrailer(a);
      if (scoreDiff !== 0) return scoreDiff;
      const aDate = Date.parse(a?.published_at || 0) || 0;
      const bDate = Date.parse(b?.published_at || 0) || 0;
      return bDate - aDate;
    });
  return sorted[0] || null;
}

async function fetchDiscoverTrailerKey(type, id) {
  const cacheKey = getDiscoverTrailerCacheKey(type, id);
  if (discoverTrailerCache.has(cacheKey)) return discoverTrailerCache.get(cacheKey);
  if (!id || (type !== 'movie' && type !== 'tv')) {
    discoverTrailerCache.set(cacheKey, null);
    return null;
  }
  try {
    const res = await fetchTmdbProxy(`${type}/${id}/videos`);
    if (!res.ok) throw new Error(`TMDB videos request failed: ${res.status}`);
    const json = await res.json();
    const trailerKey = pickBestDiscoverTrailer(json.results || [])?.key || null;
    discoverTrailerCache.set(cacheKey, trailerKey);
    return trailerKey;
  } catch (e) {
    console.error('Discover trailer fetch failed:', e);
    discoverTrailerCache.set(cacheKey, null);
    return null;
  }
}

function buildDiscoverPosterMarkup(poster) {
  return `<div class="discover-poster-media" style="background-image:url('${poster}')"></div>`;
}

function getDiscoverExpandIconMarkup(container) {
  if (!container || !container.dataset.mediaType || !container.dataset.mediaId) return '';
  return `<button class="discover-expand-icon" type="button" aria-label="Preview trailer" onclick="handleDiscoverExpandIconClick(event, this, '${container.dataset.mediaType}', ${container.dataset.mediaId})"><span></span><span></span><span></span><span></span></button>`;
}

function getDiscoverPosterTooltipMarkup() {
  return `<div class="discover-poster-tooltip">Click poster to preview trailer</div>`;
}


let discoverFriendSocialCache = null;
let discoverFriendSocialCacheKey = '';
let discoverFriendSocialPromise = null;

function normalizeDiscoverSocialTitle(title) {
  return String(title || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’']/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function getDiscoverSocialSections(section) {
  if (section === 'shows') return ['shows', 'anime'];
  return [section].filter(Boolean);
}

function getDiscoverAvatarUrl(user = {}) {
  if (user.photo) return user.photo;
  return 'https://ui-avatars.com/api/?name=' + encodeURIComponent(user.name || 'Friend') + '&background=1e2028&color=60a5fa';
}

function parseDiscoverFriendListField(raw) {
  try { return raw ? JSON.parse(raw) : []; } catch(e) { return []; }
}

function isDiscoverFriendTriggerStatus(item, section) {
  const status = String(item?.status || '').toLowerCase();
  return status === 'watching' || status === 'planned';
}

function getDiscoverFriendMatch(friend, normalizedTitle, section) {
  const sections = getDiscoverSocialSections(section);
  for (const listSection of sections) {
    const match = (friend.listData?.[listSection] || []).find(item =>
      normalizeDiscoverSocialTitle(item.title) === normalizedTitle &&
      isDiscoverFriendTriggerStatus(item, section)
    );
    if (match) return { item: match, listSection };
  }
  return null;
}

function getDiscoverSocialStatusLabel(status, section) {
  const normalized = String(status || '').toLowerCase();
  if (section === 'games') {
    if (normalized === 'live') return 'Live Games';
    if (normalized === 'watching') return 'Playing';
    if (normalized === 'planned') return 'Backlog';
  }
  if (normalized === 'watching') return 'Watching';
  if (normalized === 'planned') return 'Plan to Watch';
  return '';
}

function getDiscoverFriendStackFromContainer(container) {
  return getDiscoverFriendStackMarkup(container?.dataset?.discoverTitle || '', container?.dataset?.discoverSection || '');
}

function getDiscoverFriendMatches(title, section) {
  const normalizedTitle = normalizeDiscoverSocialTitle(title);
  if (!normalizedTitle) return [];
  const source = discoverFriendSocialCache || (isPreviewMode() ? PREVIEW_COMMUNITY_USERS.map(user => ({
    uid: user.uid,
    name: user.name,
    photo: user.photo,
    listData: cloneListData(user.listData || {})
  })) : []);

  return source.map(friend => {
    const match = getDiscoverFriendMatch(friend, normalizedTitle, section);
    return match ? { ...friend, discoverMatchItem: match.item, discoverMatchSection: match.listSection } : null;
  }).filter(Boolean);
}

function getDiscoverFriendStackMarkup(title, section) {
  const titleAttr = escAttr(title);
  const sectionAttr = escAttr(section);
  const matches = getDiscoverFriendMatches(title, section);
  if (!matches.length) {
    return `<div class="discover-friend-stack" data-discover-title="${titleAttr}" data-discover-section="${sectionAttr}" aria-hidden="true"></div>`;
  }

  const visible = matches.slice(0, 3);
  const extra = matches.length - visible.length;
  const names = matches.map(friend => friend.name || 'Friend');
  const label = names.length === 1 ? `${names[0]} added this` : `${names.slice(0, 3).join(', ')}${names.length > 3 ? ` and ${names.length - 3} more` : ''} added this`;
  const avatars = visible.map(friend => `<img class="discover-friend-avatar" src="${escAttr(getDiscoverAvatarUrl(friend))}" alt="" loading="lazy">`).join('');
  const count = extra > 0 ? `<span class="discover-friend-count">+${extra}</span>` : '';
  return `<div class="discover-friend-stack has-friends" data-discover-title="${titleAttr}" data-discover-section="${sectionAttr}" title="${escAttr(label)}" aria-label="${escAttr(label)}" role="button" tabindex="0" onclick="openDiscoverFriendsModal(event, this)" onkeydown="handleDiscoverFriendStackKeydown(event, this)" onpointerdown="event.stopPropagation()">${avatars}${count}</div>`;
}

async function loadDiscoverFriendSocialCache(force = false) {
  const cacheKey = isPreviewMode()
    ? 'preview'
    : currentUser
      ? friends.slice().sort().join('|')
      : 'signed-out';

  if (!force && discoverFriendSocialCache && discoverFriendSocialCacheKey === cacheKey) return discoverFriendSocialCache;
  if (!force && discoverFriendSocialPromise && discoverFriendSocialCacheKey === cacheKey) return discoverFriendSocialPromise;

  discoverFriendSocialCacheKey = cacheKey;
  discoverFriendSocialPromise = (async () => {
    if (isPreviewMode()) {
      discoverFriendSocialCache = PREVIEW_COMMUNITY_USERS.map(user => ({
        uid: user.uid,
        name: user.name,
        photo: user.photo,
        listData: cloneListData(user.listData || {})
      }));
      return discoverFriendSocialCache;
    }

    if (!currentUser || !friends.length) {
      discoverFriendSocialCache = [];
      return discoverFriendSocialCache;
    }

    const rows = await Promise.all(friends.map(async uid => {
      try {
        const [userSnap, listSnap] = await Promise.all([
          db.collection('users').doc(uid).get(),
          db.collection('watchlist').doc(uid).get()
        ]);
        const user = userSnap.exists ? userSnap.data() : usersMap[uid] || {};
        if (userSnap.exists) usersMap[uid] = { ...user, uid };
        const list = listSnap.exists ? listSnap.data() : {};
        return {
          uid,
          name: user.name || 'Friend',
          photo: user.photo || '',
          listData: normalizeListData({
            shows: parseDiscoverFriendListField(list.shows),
            movies: parseDiscoverFriendListField(list.movies),
            anime: parseDiscoverFriendListField(list.anime),
            games: parseDiscoverFriendListField(list.games)
          })
        };
      } catch(e) {
        return null;
      }
    }));

    discoverFriendSocialCache = rows.filter(Boolean);
    return discoverFriendSocialCache;
  })();

  try {
    return await discoverFriendSocialPromise;
  } finally {
    discoverFriendSocialPromise = null;
  }
}

function refreshDiscoverFriendStacks(force = false) {
  const stacks = document.querySelectorAll('.discover-friend-stack');
  if (!stacks.length) return;
  loadDiscoverFriendSocialCache(force).then(() => {
    document.querySelectorAll('.discover-friend-stack').forEach(stack => {
      const wrapper = document.createElement('div');
      wrapper.innerHTML = getDiscoverFriendStackMarkup(stack.dataset.discoverTitle || '', stack.dataset.discoverSection || '');
      if (wrapper.firstElementChild) stack.replaceWith(wrapper.firstElementChild);
    });
  }).catch(e => console.error('Discover friend avatars failed:', e));
}

function isDiscoverMobileViewport() {
  return window.matchMedia && window.matchMedia('(max-width: 600px)').matches;
}

function handleDiscoverFriendStackKeydown(event, stack) {
  if (event.key !== 'Enter' && event.key !== ' ') return;
  event.preventDefault();
  openDiscoverFriendsModal(event, stack);
}

function closeDiscoverFriendsModal() {
  const overlay = document.getElementById('discover-friends-modal-overlay');
  if (!overlay) return;
  overlay.classList.remove('open');
  document.removeEventListener('keydown', handleDiscoverFriendsModalEsc);
  setTimeout(() => overlay.remove(), 260);
}

function handleDiscoverFriendsModalEsc(event) {
  if (event.key === 'Escape') closeDiscoverFriendsModal();
}

function openDiscoverFriendsModal(event, stack) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  if (!stack || !isDiscoverMobileViewport()) return;

  const title = stack.dataset.discoverTitle || '';
  const section = stack.dataset.discoverSection || '';
  const matches = getDiscoverFriendMatches(title, section);
  if (!matches.length) return;

  const titleText = title ? escHtml(title) : 'this title';
  const rows = matches.map(friend => {
    const statusLabel = getDiscoverSocialStatusLabel(friend.discoverMatchItem?.status, section);
    return `<div class="discover-friends-modal-row">
      <img class="discover-friends-modal-avatar" src="${escAttr(getDiscoverAvatarUrl(friend))}" alt="${escAttr(friend.name || 'Friend')}" loading="lazy">
      <div>
        <div class="discover-friends-modal-name">${escHtml(friend.name || 'Friend')}</div>
        ${statusLabel ? `<div class="discover-friends-modal-status">${escHtml(statusLabel)}</div>` : ''}
      </div>
    </div>`;
  }).join('');

  closeDiscoverFriendsModal();
  const overlay = document.createElement('div');
  overlay.id = 'discover-friends-modal-overlay';
  overlay.className = 'discover-friends-modal-overlay';
  overlay.onclick = e => { if (e.target === overlay) closeDiscoverFriendsModal(); };
  overlay.innerHTML = `<div class="discover-friends-modal" role="dialog" aria-modal="true" aria-label="Friends who added ${escAttr(title)}">
    <div class="discover-friends-modal-head">
      <div>
        <div class="discover-friends-modal-title">Friends on ${titleText}</div>
        <div class="discover-friends-modal-subtitle">${matches.length} friend${matches.length === 1 ? '' : 's'} triggered this poster icon.</div>
      </div>
      <button class="discover-friends-modal-close" type="button" onclick="closeDiscoverFriendsModal()" aria-label="Close">×</button>
    </div>
    <div class="discover-friends-modal-rule">Icons appear when this title is in a friend’s Watching or Plan to Watch. For games, icons appear when the game is in Playing or Backlog.</div>
    <div class="discover-friends-modal-list">${rows}</div>
  </div>`;
  document.body.appendChild(overlay);
  document.addEventListener('keydown', handleDiscoverFriendsModalEsc);
  requestAnimationFrame(() => overlay.classList.add('open'));
}

function getDiscoverGenreNames(item, itemType) {
  if (Array.isArray(item?.genreNames) && item.genreNames.length) {
    return item.genreNames.map(name => String(name || '').trim()).filter(Boolean);
  }
  const genreMap = itemType === 'movie' ? DISCOVER_MOVIE_GENRE_MAP : DISCOVER_TV_GENRE_MAP;
  if (!Array.isArray(item?.genre_ids)) return [];
  return item.genre_ids.map(id => genreMap[id]).filter(Boolean);
}

function getDiscoverPosterContainer(cardOrPoster) {
  if (!cardOrPoster) return null;
  if (cardOrPoster.classList?.contains('discover-poster')) return cardOrPoster;
  return cardOrPoster.querySelector('.discover-poster');
}

function isMobileDiscoverLayout() {
  return window.matchMedia('(max-width: 700px), (hover: none) and (pointer: coarse)').matches;
}

function updateMobileDiscoverAlignment(card) {
  if (!card) return;
  card.classList.remove('mobile-expanded-left', 'mobile-expanded-right');
  card.style.removeProperty('--discover-mobile-shift-x');
  if (!isMobileDiscoverLayout() || !card.classList.contains('discover-card-expanded')) return;
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
  const cardRect = card.getBoundingClientRect();
  const expandedWidth = Math.min(viewportWidth - 24, 520);
  const viewportCenter = viewportWidth / 2;
  const cardCenter = cardRect.left + (cardRect.width / 2);
  const baseLeft = cardCenter - (expandedWidth / 2);
  const baseRight = cardCenter + (expandedWidth / 2);
  const minShift = 12 - baseLeft;
  const maxShift = (viewportWidth - 12) - baseRight;
  const idealShift = viewportCenter - cardCenter;
  const clampedShift = Math.max(minShift, Math.min(maxShift, idealShift));
  card.style.setProperty('--discover-mobile-shift-x', `${clampedShift}px`);
  card.classList.add(cardCenter < viewportCenter ? 'mobile-expanded-left' : 'mobile-expanded-right');
}

function resetDiscoverPoster(cardOrPoster) {
  const container = getDiscoverPosterContainer(cardOrPoster);
  if (!container) return;
  const poster = container.dataset.poster || '';
  container.classList.remove('trailer-active');
  container.innerHTML = `${buildDiscoverPosterMarkup(poster)}${getDiscoverExpandIconMarkup(container)}${getDiscoverPosterTooltipMarkup()}${getDiscoverFriendStackFromContainer(container)}`;
}

function activateDiscoverTrailer(cardOrPoster, trailerKey) {
  const container = getDiscoverPosterContainer(cardOrPoster);
  if (!container || !trailerKey) return;
  const poster = container.dataset.poster || '';
  const isMobile = isMobileDiscoverLayout();
  const src = `https://www.youtube.com/embed/${trailerKey}?autoplay=1&mute=${isMobile ? '1' : '0'}&controls=1&playsinline=1&rel=0&modestbranding=1&enablejsapi=1`;
  container.innerHTML = `
    ${buildDiscoverPosterMarkup(poster)}
    ${getDiscoverExpandIconMarkup(container)}
    ${getDiscoverPosterTooltipMarkup()}
    ${getDiscoverFriendStackFromContainer(container)}
    <iframe class="discover-poster-video" src="${src}" allow="autoplay; encrypted-media; picture-in-picture" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
  `;
  requestAnimationFrame(() => container.classList.add('trailer-active'));
}

function closePinnedDiscoverCard(card = activeDiscoverPinnedCard) {
  if (!card) return;
  const poster = getDiscoverPosterContainer(card);
  if (poster) {
    poster.dataset.hovering = '0';
    poster.dataset.pinned = '0';
  }
  card.classList.remove('discover-card-expanded');
  card.classList.remove('mobile-expanded-left', 'mobile-expanded-right');
  card.style.removeProperty('--discover-mobile-shift-x');
  resetDiscoverPoster(card);
  if (activeDiscoverPinnedCard === card) activeDiscoverPinnedCard = null;
}

async function handleDiscoverCardHover(card, type, id) {
  const container = getDiscoverPosterContainer(card);
  if (!container || container.dataset.hovering === '0') return;
  const trailerKey = await fetchDiscoverTrailerKey(type, id);
  if (container.dataset.hovering === '0' || !trailerKey) return;
  activateDiscoverTrailer(card, trailerKey);
}

function startDiscoverCardHover(card, type, id, force = false) {
  if (!card || (type !== 'movie' && type !== 'tv')) return;
  if (isMobileDiscoverLayout() && !force) return;
  const container = getDiscoverPosterContainer(card);
  if (!container) return;
  container.dataset.hovering = '1';
  card.classList.add('discover-card-expanded');
  updateMobileDiscoverAlignment(card);
  handleDiscoverCardHover(card, type, id);
}

function stopDiscoverCardHover(card, force = false) {
  const container = getDiscoverPosterContainer(card);
  if (!container) return;
  if (isMobileDiscoverLayout() && !force) return;
  if (container.dataset.pinned === '1') return;
  container.dataset.hovering = '0';
  if (card) card.classList.remove('discover-card-expanded');
  resetDiscoverPoster(card);
}

function toggleDiscoverCardPin(event, card, type, id) {
  if (!card || (type !== 'movie' && type !== 'tv')) return;
  if (event?.target?.closest('.discover-add-btn, button, a')) return;
  const container = getDiscoverPosterContainer(card);
  if (!container) return;
  const isPinned = container.dataset.pinned === '1';
  if (isPinned) {
    closePinnedDiscoverCard(card);
    return;
  }
  if (activeDiscoverPinnedCard && activeDiscoverPinnedCard !== card) {
    closePinnedDiscoverCard(activeDiscoverPinnedCard);
  }
  activeDiscoverPinnedCard = card;
  container.dataset.pinned = '1';
  startDiscoverCardHover(card, type, id, true);
}

function clearDiscoverCardPressTimer() {
  if (discoverCardPressTimer) {
    clearTimeout(discoverCardPressTimer);
    discoverCardPressTimer = null;
  }
  if (discoverCardPressPoster) {
    discoverCardPressPoster.dataset.longPressTriggered = '0';
    discoverCardPressPoster = null;
  }
  discoverCardPressStartX = 0;
  discoverCardPressStartY = 0;
}

function startDiscoverPosterPress(event, poster, type, id) {
  if (!poster || (event?.pointerType !== 'touch' && event?.pointerType !== 'pen')) return;
  clearDiscoverCardPressTimer();
  poster.dataset.longPressTriggered = '0';
  discoverCardPressPoster = poster;
  discoverCardPressStartX = event.clientX || 0;
  discoverCardPressStartY = event.clientY || 0;
  discoverCardPressTimer = setTimeout(() => {
    if (!discoverCardPressPoster) return;
    discoverCardPressPoster.dataset.longPressTriggered = '1';
    const card = discoverCardPressPoster.closest('.discover-card');
    toggleDiscoverCardPin(null, card, type, id);
  }, discoverCardLongPressMs);
}

function stopDiscoverPosterPress() {
  if (!discoverCardPressTimer) return;
  clearTimeout(discoverCardPressTimer);
  discoverCardPressTimer = null;
  discoverCardPressStartX = 0;
  discoverCardPressStartY = 0;
  if (discoverCardPressPoster?.dataset.longPressTriggered !== '1') {
    discoverCardPressPoster = null;
  }
}

function moveDiscoverPosterPress(event) {
  if (!discoverCardPressTimer) return;
  const deltaX = Math.abs((event?.clientX || 0) - discoverCardPressStartX);
  const deltaY = Math.abs((event?.clientY || 0) - discoverCardPressStartY);
  if (deltaX > discoverCardPressMoveThreshold || deltaY > discoverCardPressMoveThreshold) {
    clearDiscoverCardPressTimer();
  }
}

function handleDiscoverPosterClick(event, poster, type, id) {
  if (!poster) return;
  event?.stopPropagation?.();
  if (isMobileDiscoverLayout()) {
    if (poster.dataset.longPressTriggered === '1') {
      poster.dataset.longPressTriggered = '0';
    }
    return;
  }
  if (poster.dataset.longPressTriggered === '1') {
    poster.dataset.longPressTriggered = '0';
    return;
  }
  const card = poster.closest('.discover-card');
  toggleDiscoverCardPin(event, card, type, id);
}

function handleDiscoverCardClick(event, card, type, id) {
  if (!card || (type !== 'movie' && type !== 'tv')) return;
  if (isMobileDiscoverLayout()) return;
  if (event?.target?.closest('.discover-add-btn, button, a, .discover-poster')) return;
  toggleDiscoverCardPin(event, card, type, id);
}

function handleDiscoverCardBodyTap(event, body) {
  if (!body || !isMobileDiscoverLayout()) return;
  const card = body.closest('.discover-card');
  if (!card?.classList.contains('discover-card-expanded')) return;
  if (event?.target?.closest('button, a, iframe, .discover-poster-video, .discover-add-btn, [role="button"]')) return;
  closePinnedDiscoverCard(card);
}

function handleDiscoverCloseClick(event, button) {
  event?.stopPropagation?.();
  const card = button?.closest('.discover-card');
  if (!card) return;
  closePinnedDiscoverCard(card);
}

function handleDiscoverExpandIconClick(event, button, type, id) {
  event?.stopPropagation?.();
  if (!isMobileDiscoverLayout()) return;
  const card = button?.closest('.discover-card');
  if (!card) return;
  toggleDiscoverCardPin(null, card, type, id);
}

async function loadGamesDiscoverSection(kind, gridId) {
  try {
    const items = await fetchGamesDiscoverTitles(kind);
    renderGamesDiscoverCards(items, gridId);
  } catch(e) {
    console.error(`Games Discovery ${kind} load failed:`, e);
    renderGamesDiscoverSectionError(gridId);
  }
}

async function loadGamesDiscover(force = false) {
  if (gamesDiscoverLoading || (gamesDiscoverLoaded && !force)) return;
  gamesDiscoverLoading = true;
  renderGamesDiscoverLoading();
  try {
    const sections = [
      ['popular', 'discover-games-popular-grid'],
      ['trending', 'discover-games-trending-grid'],
      ['new-releases', 'discover-games-new-releases-grid'],
      ['anticipated', 'discover-games-anticipated-grid'],
      ['rated', 'discover-games-rated-grid'],
      ['story', 'discover-games-story-grid'],
      ['multiplayer', 'discover-games-multiplayer-grid'],
      ['hidden', 'discover-games-hidden-grid']
    ];
    for (const [kind, gridId] of sections) {
      await loadGamesDiscoverSection(kind, gridId);
    }
    gamesDiscoverLoaded = true;
  } catch(e) {
    console.error("Games Discovery load failed:", e);
    renderGamesDiscoverError("Games Discovery could not load. Try refreshing.");
  } finally {
    gamesDiscoverLoading = false;
  }
}

function renderDiscoverCards(type, items, gridId) {
  const grid = document.getElementById(gridId || (type === 'movie' ? 'discover-movies-grid' : 'discover-tv-grid'));
  if (!grid) return;
  if (!items.length) {
    grid.innerHTML = '<div class="discover-message">No discovery titles found.</div>';
    return;
  }
  grid.dataset.expanded = 'false';
  grid.innerHTML = items.map(item => {
    const itemType = type === 'mixed' ? (item.media_type || 'movie') : type;
    const title = item.title || item.name || '';
    const year = (item.release_date || item.first_air_date || '').slice(0, 4);
    const score = item.vote_average ? Number(item.vote_average).toFixed(1) : 'N/A';
    const genreLine = getDiscoverGenreNames(item, itemType).slice(0, 2).join(' · ');
    const poster = `https://image.tmdb.org/t/p/w342${item.poster_path}`;
    const overview = item.overview || '';
    const section = itemType === 'movie' ? 'movies' : 'shows';
    const typeLabel = type === 'mixed' ? `${itemType === 'movie' ? 'Movie' : 'TV Show'} · ` : '';
    const alreadyAdded = isDuplicateTitle(title, section);
    const titleAttr = escAttr(title);
    const addClick = `openDiscoveryAddModal('${itemType}', ${item.id}, this)`;
    const removeClick = `removeDiscoveryTitle(this)`;
    return `<div class="discover-card">
      <div class="discover-poster" data-poster="${escAttr(poster)}" data-media-type="${itemType}" data-media-id="${item.id}" data-discover-title="${titleAttr}" data-discover-section="${section}" data-hovering="0" data-pinned="0" data-long-press-triggered="0" onclick="handleDiscoverPosterClick(event, this, '${itemType}', ${item.id})" onpointerdown="startDiscoverPosterPress(event, this, '${itemType}', ${item.id})" onpointermove="moveDiscoverPosterPress(event)" onpointerup="stopDiscoverPosterPress()" onpointercancel="clearDiscoverCardPressTimer()" onpointerleave="clearDiscoverCardPressTimer()">
        ${buildDiscoverPosterMarkup(poster)}${getDiscoverExpandIconMarkup({ dataset: { mediaType: itemType, mediaId: String(item.id) } })}${getDiscoverPosterTooltipMarkup()}${getDiscoverFriendStackMarkup(title, section)}
      </div>
      <div class="discover-card-body" onclick="handleDiscoverCardBodyTap(event, this)">
        <div class="discover-card-info-row">
          <div class="discover-card-info-stack">
            <div class="discover-card-title">${escHtml(title)}${year ? ` (${year})` : ''}</div>
            ${genreLine ? `<div class="discover-card-genre">${escHtml(genreLine)}</div>` : ''}
            <div class="discover-card-meta">${typeLabel}<span class="discover-rating-meta"><span class="discover-rating-star">★</span><span>${score}</span></span></div>
          </div>
          <button class="discover-close-btn" type="button" onclick="handleDiscoverCloseClick(event, this)">Close</button>
        </div>
        ${overview ? `<div class="discover-card-overview">${escHtml(overview)}</div>` : ''}
        <button class="discover-add-btn${alreadyAdded ? ' added' : ''}" data-discover-type="${itemType}" data-discover-id="${item.id}" data-discover-section="${section}" data-discover-title="${titleAttr}" title="${alreadyAdded ? 'Click to remove from your library' : ''}" onclick="${alreadyAdded ? removeClick : addClick}">${alreadyAdded ? 'Added' : '+ Add to Library'}</button>
      </div>
    </div>`;
  }).join('');
  requestAnimationFrame(() => {
    setupDiscoverSectionLimit(grid);
    refreshDiscoverFriendStacks();
  });
}

function renderGamesDiscoverCards(items, gridId) {
  const grid = document.getElementById(gridId);
  if (!grid) return;
  if (!items.length) {
    grid.innerHTML = '<div class="discover-message">No game discovery titles found.</div>';
    return;
  }
  grid.dataset.expanded = 'false';
  grid.innerHTML = items.map(item => {
    const title = item.name || '';
    const year = (item.released || '').slice(0, 4);
    const score = item.rating ? `${Number(item.rating).toFixed(1)}/5` : 'No rating';
    const poster = item.background_image || '';
    const genres = (item.genres || []).map(g => g.name).slice(0, 3).join(', ');
    const platforms = (item.platforms || []).map(p => p.platform?.name).filter(Boolean).slice(0, 3).join(', ');
    const overview = genres || platforms || 'Game';
    const alreadyAdded = isDuplicateTitle(title, 'games');
    const titleAttr = escAttr(title);
    const addClick = `openDiscoveryAddModal('game', ${item.id}, this)`;
    const removeClick = `removeDiscoveryTitle(this)`;
    return `<div class="discover-card games-discover-card">
      <div class="discover-poster" data-discover-title="${titleAttr}" data-discover-section="games" style="background-image:url('${poster}')">${getDiscoverFriendStackMarkup(title, 'games')}</div>
      <div class="discover-card-body">
        <div class="discover-card-title">${escHtml(title)}${year ? ` (${year})` : ''}</div>
        <div class="discover-card-meta">${escHtml(score)}${platforms ? ` · ${escHtml(platforms)}` : ''}</div>
        <div class="discover-card-overview">${escHtml(overview)}</div>
        <button class="discover-add-btn${alreadyAdded ? ' added' : ''}" data-discover-type="game" data-discover-id="${item.id}" data-discover-section="games" data-discover-title="${titleAttr}" title="${alreadyAdded ? 'Click to remove from your library' : ''}" onclick="${alreadyAdded ? removeClick : addClick}">${alreadyAdded ? 'Added' : '+ Add to Library'}</button>
      </div>
    </div>`;
  }).join('');
  requestAnimationFrame(() => {
    setupDiscoverSectionLimit(grid);
    refreshDiscoverFriendStacks();
  });
}

function getDiscoverTwoRowLimit(grid) {
  const columns = getComputedStyle(grid).gridTemplateColumns.split(' ').filter(Boolean).length;
  return Math.max(1, columns || 1) * 2;
}

function setupDiscoverSectionLimit(grid) {
  if (!grid) return;
  const cards = Array.from(grid.querySelectorAll('.discover-card'));
  const button = getDiscoverExpandButton(grid);
  if (!button) return;
  const limit = getDiscoverTwoRowLimit(grid);
  const expanded = grid.dataset.expanded === 'true';
  cards.forEach((card, index) => {
    card.classList.toggle('discover-hidden', !expanded && index >= limit);
  });
  button.style.display = cards.length > limit ? '' : 'none';
  button.textContent = expanded ? 'Show less' : 'Show more';
  button.classList.toggle('is-collapsing', expanded);
}

function toggleDiscoverSection(gridId) {
  const grid = document.getElementById(gridId);
  if (!grid) return;
  const wasExpanded = grid.dataset.expanded === 'true';
  grid.dataset.expanded = wasExpanded ? 'false' : 'true';
  setupDiscoverSectionLimit(grid);
  if (wasExpanded) grid.closest('.discover-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function jumpToDiscoverSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (!section) return;
  section.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

let discoverResizeTimer = null;
window.addEventListener('resize', () => {
  clearTimeout(discoverResizeTimer);
  discoverResizeTimer = setTimeout(() => {
    getAllDiscoverGrids().forEach(grid => {
      if (grid.dataset.expanded !== 'true') setupDiscoverSectionLimit(grid);
    });
  }, 120);
});

let pendingDiscoveryAdd = null;

function openDiscoveryAddModal(type, tmdbId, btn) {
  if (!btn || btn.disabled) return;
  pendingDiscoveryAdd = { type, tmdbId, btn, originalText: btn.textContent };
  renderDiscoveryAddChoice();
  document.getElementById('discover-add-modal').style.display = 'flex';
}

function renderDiscoveryAddChoice() {
  const content = document.getElementById('discover-add-modal-content');
  if (!content) return;
  const isGame = pendingDiscoveryAdd?.type === 'game';
  const watchedLabel = isGame ? 'Completed' : 'Watched';
  const plannedLabel = isGame ? 'Backlog' : 'Planned to Watch';
  content.innerHTML = `
    <h3>Add to Library</h3>
    <div class="discover-add-desc">Where you bouta put this?</div>
    <div class="discover-status-options">
      <button class="discover-status-btn watched-option" onclick="confirmDiscoveryAdd('watched')">${watchedLabel}</button>
      <button class="discover-status-btn planned-option" onclick="confirmDiscoveryAdd('planned')">${plannedLabel}</button>
    </div>
    <div class="modal-actions">
      <button class="btn-secondary discover-cancel-btn" onclick="closeDiscoverAddModal()">Cancel</button>
    </div>
  `;
}

function closeDiscoverAddModal() {
  document.getElementById('discover-add-modal').style.display = 'none';
  pendingDiscoveryAdd = null;
  pendingFriendAdd = null;
}

function confirmDiscoveryAdd(status) {
  if (!pendingDiscoveryAdd) return;
  if (status === 'watched') {
    renderDiscoveryRatingPrompt(0);
    return;
  }
  finalizeDiscoveryAdd(status, 0);
}

function renderDiscoveryRatingPrompt(selectedRating = 0) {
  const content = document.getElementById('discover-add-modal-content');
  if (!content) return;
  const isGame = pendingDiscoveryAdd?.type === 'game';
  const skipLabel = isGame ? 'completed' : 'watched';
  let stars = '';
  for (let i = 1; i <= 10; i++) {
    stars += `<button class="star-btn ${i <= selectedRating ? 'lit' : ''}"
      onclick="selectDiscoveryRating(${i})"
      onmouseenter="hoverStars(this,${i})"
      onmouseleave="unhoverStars(this,${selectedRating})">★</button>`;
  }
  content.innerHTML = `
    <div class="discover-rating-prompt">
      <h3>Rate this Title</h3>
      <div class="discover-add-desc">Choose a rating, or skip and add it as ${skipLabel}.</div>
      <div class="stars discover-rating-stars" data-discover-rating="${selectedRating}">${stars}${selectedRating > 0 ? `<span class="star-label">${selectedRating}</span>` : ''}</div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="renderDiscoveryAddChoice()">Back</button>
        <button class="btn-secondary" onclick="finalizeDiscoveryAdd('watched', 0)">Skip</button>
      </div>
    </div>
  `;
}

function selectDiscoveryRating(score) {
  if (!pendingDiscoveryAdd || pendingDiscoveryAdd.ratingLock) return;
  pendingDiscoveryAdd.ratingLock = true;
  const container = document.querySelector('#discover-add-modal .discover-rating-stars');
  if (container) {
    container.dataset.discoverRating = score;
    container.querySelectorAll('.star-btn').forEach((star, index) => {
      const lit = index + 1 <= score;
      star.classList.toggle('lit', lit);
      star.style.color = lit ? '#f59e0b' : '#443d60';
      star.style.transform = 'scale(1)';
    });
    let label = container.querySelector('.star-label');
    if (!label) {
      label = document.createElement('span');
      label.className = 'star-label';
      container.appendChild(label);
    }
    label.textContent = score;
    const animationMs = playDiscoveryModalRatingAnimation(score, container);
    setTimeout(() => finalizeDiscoveryAdd('watched', score), animationMs);
    return;
  }
  finalizeDiscoveryAdd('watched', score);
}

function playDiscoveryModalRatingAnimation(score, container) {
  if (!container || score < 1) return 0;

  const t = Math.pow(score / 10, 1.3);
  const peakScale = 1.3 + t * 0.7;
  const midScale  = 1.05 + t * 0.18;
  const glow      = 5 + t * 16;
  const glowAlpha = 0.5 + t * 0.5;
  const stagger   = (0.07 - t * 0.04) * 1000;
  const duration  = 380 + t * 240;
  const isPerfect = score === 10;

  const glowR = Math.round(251 - t * 15);
  const glowG = Math.round(191 - t * 119);
  const glowB = Math.round(36 + t * 117);
  const peakFilter = `drop-shadow(0 0 ${glow}px rgba(${glowR},${glowG},${glowB},${glowAlpha}))`;

  requestAnimationFrame(() => {
    const lit = [...container.querySelectorAll('.star-btn.lit')];
    lit.forEach((star, i) => {
      star.style.willChange = 'transform, filter';
      const anim = star.animate([
        { transform: 'scale(1)', filter: 'none' },
        { transform: `scale(${peakScale})`, filter: peakFilter, offset: 0.3 },
        { transform: `scale(${midScale})`, filter: 'none', offset: 0.6 },
        { transform: 'scale(1)', filter: 'none' }
      ], { duration, delay: i * stagger, easing: 'ease-out', fill: 'none' });
      anim.onfinish = () => { star.style.willChange = ''; };
    });

    const label = container.querySelector('.star-label');
    if (label) {
      label.style.willChange = 'transform, color';
      const lAnim = label.animate([
        { transform: 'scale(1)', color: '' },
        { transform: `scale(${1.15 + t * 0.35})`, color: '#fbbf24', offset: 0.4 },
        { transform: 'scale(1)', color: '' }
      ], { duration: 500 + t * 180, delay: 100 + t * 70, easing: 'ease-out' });
      lAnim.onfinish = () => { label.style.willChange = ''; };
    }

    if (isPerfect) spawnPerfectBurst(container);
  });

  return Math.min(960, Math.ceil(duration + Math.max(0, score - 1) * stagger + 80));
}

function finalizeDiscoveryAdd(status, rating = 0) {
  if (!pendingDiscoveryAdd) return;
  const pending = pendingDiscoveryAdd;
  document.getElementById('discover-add-modal').style.display = 'none';
  pendingDiscoveryAdd = null;
  addDiscoveryTitle(pending.type, pending.tmdbId, pending.btn, status, pending.originalText, rating);
}

function markDiscoverButtonAdded(btn) {
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = 'Added';
  btn.classList.add('added');
}

async function addDiscoveryTitle(type, tmdbId, btn, status = 'planned', originalText = '', rating = 0) {
  if (btn) {
    btn.disabled = true;
    btn.classList.remove('added');
    btn.textContent = 'Adding...';
  }
  try {
    const item = type === 'game'
      ? await buildRawgLibraryItem(tmdbId, status, rating)
      : await buildTmdbLibraryItem(type, tmdbId, status, rating);
    const section = type === 'movie'
      ? 'movies'
      : type === 'game'
        ? 'games'
        : resolveShowSection(item, item.mediaCategory || 'shows');

    // While viewing a friend, save() bails — write directly to own Firestore doc.
    if (viewingUser) {
      const targetData = myData
        ? cloneListData(myData)
        : (ownDataCache ? cloneListData(ownDataCache) : await loadOwnDataFromFirestore());
      if (isDuplicateTitleInList(item.title, section, targetData)) {
        showToast("this title is already added to your library silly!");
        markDiscoverButtonAdded(btn);
        return;
      }
      targetData[section] = Array.isArray(targetData[section]) ? targetData[section] : [];
      targetData[section].push(item);
      await writeOwnDataDirect(targetData);
      myData = cloneListData(targetData);
      markDiscoverButtonAdded(btn);
      if (btn) {
        btn.dataset.discoverType = type;
        btn.dataset.discoverId = String(tmdbId);
        btn.dataset.discoverSection = section;
        btn.dataset.discoverTitle = item.title;
        btn.setAttribute('onclick', 'removeDiscoveryTitle(this)');
        btn.disabled = false;
        btn.title = 'Click to remove from your library';
      }
      showToast("Added to your library");
      return;
    }

    if (isDuplicateTitle(item.title, section)) {
      showToast("this title is already added to your library silly!");
      markDiscoverButtonAdded(btn);
      return;
    }
    data[section].push(item);
    activeSection = section;
    activeTab = status;
    save();
    render();
    if (rating > 0) {
      requestAnimationFrame(() => playRatingAnimation(item.id, 'overall'));
    }
    markDiscoverButtonAdded(btn);
    if (btn) {
      btn.dataset.discoverType = type;
      btn.dataset.discoverId = String(tmdbId);
      btn.dataset.discoverSection = section;
      btn.dataset.discoverTitle = item.title;
      btn.setAttribute('onclick', 'removeDiscoveryTitle(this)');
      btn.disabled = false;
      btn.title = 'Click to remove from your library';
    }
    showToast("Added to your library");
  } catch(e) {
    console.error("Discover add failed:", e);
    if (btn) {
      btn.disabled = false;
      btn.classList.remove('added');
      btn.textContent = originalText || '+ Add to Library';
    }
    showToast("Could not add this title. Try again.");
  }
}

async function removeDiscoveryTitle(btn) {
  if (!btn) return;
  const section = btn.dataset.discoverSection;
  const title = btn.dataset.discoverTitle || '';
  if (!section || !title) return;
  const titleLower = title.trim().toLowerCase();

  const originalLabel = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Removing...';

  try {
    if (viewingUser) {
      // Bypass save() bail: write directly to own Firestore.
      const targetData = myData
        ? cloneListData(myData)
        : (ownDataCache ? cloneListData(ownDataCache) : await loadOwnDataFromFirestore());
      const list = Array.isArray(targetData[section]) ? targetData[section] : [];
      const idx = list.findIndex(it => (it?.title || '').trim().toLowerCase() === titleLower);
      if (idx === -1) {
        // Stale state: nothing to remove. Just reset the button.
        resetDiscoverButton(btn);
        showToast("Already not in your library");
        return;
      }
      list.splice(idx, 1);
      targetData[section] = list;
      await writeOwnDataDirect(targetData);
      myData = cloneListData(targetData);
      resetDiscoverButton(btn);
      showToast("Removed from your library");
      return;
    }

    const list = Array.isArray(data[section]) ? data[section] : [];
    const idx = list.findIndex(it => (it?.title || '').trim().toLowerCase() === titleLower);
    if (idx === -1) {
      resetDiscoverButton(btn);
      showToast("Already not in your library");
      return;
    }
    data[section].splice(idx, 1);
    save();
    render();
    resetDiscoverButton(btn);
    showToast("Removed from your library");
  } catch(e) {
    console.error("Discover remove failed:", e);
    btn.disabled = false;
    btn.textContent = originalLabel || 'Added';
    showToast("Could not remove. Try again.");
  }
}

function resetDiscoverButton(btn) {
  if (!btn) return;
  const type = btn.dataset.discoverType;
  const discoverId = btn.dataset.discoverId;
  btn.classList.remove('added');
  btn.disabled = false;
  btn.textContent = '+ Add to Library';
  btn.removeAttribute('title');
  if (type && discoverId) {
    btn.setAttribute('onclick', `openDiscoveryAddModal('${type}', ${JSON.stringify(discoverId)}, this)`);
    return;
  }
  btn.removeAttribute('onclick');
}


async function buildRawgLibraryItem(rawgId, status = 'planned', rating = 0) {
  const res = await fetchRawgProxy(`games/${rawgId}`);
  if (!res.ok) throw new Error("RAWG details request failed");
  const d = await res.json();
  return {
    id: Date.now().toString() + '-rawg-' + rawgId,
    title: d.name || '',
    cover: d.background_image || '',
    genre: (d.genres || []).map(g => g.name).join(', '),
    year: (d.released || '').slice(0, 4),
    status,
    rating,
    dateAdded: new Date().toISOString(),
    imdbId: '',
    platforms: (d.platforms || []).map(p => p.platform?.name).filter(Boolean).join(', '),
    metacriticSlug: d.slug || '',
    tmdbId: '',
    episodes: []
  };
}

async function buildTmdbLibraryItem(type, tmdbId, status = 'planned', rating = 0) {
  const res = await fetchTmdbProxy(`${type}/${tmdbId}`);
  if (!res.ok) throw new Error("TMDB details request failed");
  const d = await res.json();
  const title = d.title || d.name || '';
  const item = {
    id: Date.now().toString() + '-' + tmdbId,
    title,
    cover: d.poster_path ? `https://image.tmdb.org/t/p/w500${d.poster_path}` : '',
    genre: (d.genres || []).map(g => g.name).join(', '),
    year: (d.release_date || d.first_air_date || '').slice(0, 4),
    status,
    rating,
    dateAdded: new Date().toISOString(),
    imdbId: d.imdb_id || '',
    platforms: '',
    metacriticSlug: '',
    tmdbId: String(tmdbId),
    genreNames: (d.genres || []).map(g => g.name).filter(Boolean),
    originalTitle: d.original_name || d.original_title || '',
    originalLanguage: d.original_language || '',
    originCountries: Array.isArray(d.origin_country) ? d.origin_country : [],
  };

  if (type === "tv") {
    item.mediaCategory = detectAnimeFromMetadata(item) ? 'anime' : 'shows';
    item.librarySection = item.mediaCategory;
    item.isAnime = item.mediaCategory === 'anime';
    try {
      const extRes = await fetchTmdbProxy(`tv/${tmdbId}/external_ids`);
      const extData = await extRes.json();
      if (extData.imdb_id) item.imdbId = extData.imdb_id;
    } catch(e) {}

    const seasons = (d.seasons || []).filter(s => s.season_number > 0);
    let allEpisodes = [];
    for (const season of seasons) {
      try {
        const sRes = await fetchTmdbProxy(`tv/${tmdbId}/season/${season.season_number}`);
        const sData = await sRes.json();
        (sData.episodes || []).forEach(ep => {
          allEpisodes.push({
            id: item.id + '-ep-' + (allEpisodes.length + 1),
            number: allEpisodes.length + 1,
            seasonNum: season.season_number,
            epNum: ep.episode_number,
            title: ep.name || '',
            watched: status === 'watched',
            rating: 0,
          });
        });
      } catch(e) {}
    }
    item.totalEpisodes = allEpisodes.length;
    item.episodes = allEpisodes;
  } else {
    item.mediaCategory = 'movies';
    item.librarySection = 'movies';
    item.isAnime = false;
  }

  return item;
}


// Main nav
function getMainNavPanels(tab) {
  if (tab === 'mylist') return [document.getElementById('mylist-header'), document.getElementById('mylist-view')];
  if (tab === 'community') return [document.getElementById('community-view')];
  if (tab === 'discover') return [document.getElementById('discover-view')];
  if (tab === 'games-discover') return [document.getElementById('games-discover-view')];
  if (tab === 'profile') return [document.getElementById('profile-page')];
  return [];
}

function resetPanelStyles(elements) {
  elements.forEach(el => {
    if (!el) return;
    el.style.opacity = '';
    el.style.transform = '';
    el.style.filter = '';
    el.style.willChange = '';
  });
}

function setMainNavVisibility(tab) {
  const myListView = document.getElementById('mylist-view');
  const myListHeader = document.getElementById('mylist-header');
  const communityView = document.getElementById('community-view');
  const discoverView = document.getElementById('discover-view');
  const gamesDiscoverView = document.getElementById('games-discover-view');
  const profilePage = document.getElementById('profile-page');
  resetPanelStyles([myListView, myListHeader, communityView, discoverView, gamesDiscoverView, profilePage]);
  if (myListView) myListView.style.display = tab === 'mylist' ? 'block' : 'none';
  if (myListHeader) myListHeader.style.display = tab === 'mylist' ? 'block' : 'none';
  if (communityView) communityView.style.display = tab === 'community' ? 'block' : 'none';
  if (discoverView) discoverView.style.display = tab === 'discover' ? 'block' : 'none';
  if (gamesDiscoverView) gamesDiscoverView.style.display = tab === 'games-discover' ? 'block' : 'none';
  if (profilePage) profilePage.style.display = tab === 'profile' ? 'block' : 'none';
}

function getActiveMainTab() {
  const navGamesDiscover = document.getElementById('nav-games-discover');
  const navDiscover = document.getElementById('nav-discover');
  const navCommunity = document.getElementById('nav-community');
  if (navGamesDiscover?.classList.contains('active')) return 'games-discover';
  if (navDiscover?.classList.contains('active')) return 'discover';
  if (navCommunity?.classList.contains('active')) return 'community';
  return 'mylist';
}

function persistUiState() {
  try {
    const activityOpen = !!document.getElementById('activity-page')?.classList.contains('active');
    const state = {
      mainTab: getActiveMainTab(),
      activeSection,
      activeTab,
      activeFriendsTab,
      viewingUser: viewingUser ? { uid: viewingUser.uid, name: viewingUser.name, photo: viewingUser.photo || '' } : null,
      activityOpen,
      commentsViewState: commentsViewState ? { ...commentsViewState } : null
    };
    localStorage.setItem(UI_STATE_KEY, JSON.stringify(state));
  } catch (e) {
    console.error('UI state persist failed:', e);
  }
}

function readUiState() {
  try {
    const raw = localStorage.getItem(UI_STATE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    console.error('UI state read failed:', e);
    return null;
  }
}

async function restoreUiState() {
  const state = readUiState();
  if (!state) return;

  if (state.activeSection && ['shows', 'movies', 'anime', 'games'].includes(state.activeSection)) {
    activeSection = state.activeSection;
  }
  if (state.activeTab && ['watching', 'planned', 'watched', 'paused', 'dropped', 'live'].includes(state.activeTab)) {
    activeTab = state.activeTab;
  }
  if (state.activeFriendsTab && ['friends', 'find', 'requests'].includes(state.activeFriendsTab)) {
    activeFriendsTab = state.activeFriendsTab;
  }

  const mainTab = ['mylist', 'community', 'discover', 'games-discover'].includes(state.mainTab) ? state.mainTab : 'mylist';
  const navMyList = document.getElementById('nav-mylist');
  const navCommunity = document.getElementById('nav-community');
  const navDiscover = document.getElementById('nav-discover');
  const navGamesDiscover = document.getElementById('nav-games-discover');
  if (navMyList) navMyList.classList.toggle('active', mainTab === 'mylist');
  if (navCommunity) navCommunity.classList.toggle('active', mainTab === 'community');
  if (navDiscover) navDiscover.classList.toggle('active', mainTab === 'discover');
  if (navGamesDiscover) navGamesDiscover.classList.toggle('active', mainTab === 'games-discover');

  render();
  setMainNavVisibility(mainTab);

  if (state.viewingUser?.uid && currentUser && state.viewingUser.uid !== currentUser.uid) {
    await viewUserList(state.viewingUser.uid, state.viewingUser.name || 'Friend', state.viewingUser.photo || '');
  } else {
    if (mainTab === 'community') {
      loadCommunity();
      switchFriendsTab(activeFriendsTab || 'friends');
    }
    if (mainTab === 'discover') loadDiscover();
    if (mainTab === 'games-discover') loadGamesDiscover();
  }

  if (state.activityOpen && mainTab === 'discover') {
    openActivityPage();
  }

  if (state.commentsViewState?.type === 'item' && state.commentsViewState.itemId) {
    openCommentsPage(state.commentsViewState.itemId, null);
  } else if (state.commentsViewState?.type === 'activity' && state.commentsViewState.mediaKey) {
    openCommentsPageForActivity(
      state.commentsViewState.mediaKey,
      state.commentsViewState.title || 'Comments',
      state.commentsViewState.cover || '',
      state.commentsViewState.commentId || ''
    );
  }
}

function animateMainNavPanels(elements, keyframes, options) {
  const visible = elements.filter(el => el && getComputedStyle(el).display !== 'none');
  if (visible.length === 0 || !Element.prototype.animate) return Promise.resolve();
  return Promise.all(visible.map(el => {
    el.style.willChange = 'opacity, transform, filter';
    const animation = el.animate(keyframes, options);
    const timeout = new Promise(resolve => setTimeout(resolve, (options.duration || 0) + 120));
    return Promise.race([animation.finished.catch(() => {}), timeout])
      .catch(() => {})
      .finally(() => {
        try { animation.cancel(); } catch(e) {}
        el.style.willChange = '';
      });
  }));
}

async function switchMainNav(tab) {
  const navMyList = document.getElementById('nav-mylist');
  const navCommunity = document.getElementById('nav-community');
  const navDiscover = document.getElementById('nav-discover');
  const navGamesDiscover = document.getElementById('nav-games-discover');
  if (!navMyList || !navCommunity || !navDiscover || !navGamesDiscover) return;
  const currentTab = navGamesDiscover.classList.contains('active') ? 'games-discover' : navDiscover.classList.contains('active') ? 'discover' : navCommunity.classList.contains('active') ? 'community' : 'mylist';
  if (mainNavSwitching) return;
  if (tab === currentTab) {
    if (tab === 'mylist' && viewingUser) await backToMyList();
    return;
  }

  navMyList.classList.toggle('active', tab === 'mylist');
  navCommunity.classList.toggle('active', tab === 'community');
  navDiscover.classList.toggle('active', tab === 'discover');
  navGamesDiscover.classList.toggle('active', tab === 'games-discover');

  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  mainNavSwitching = true;
  try {
    if (!prefersReducedMotion) {
      await animateMainNavPanels(
        getMainNavPanels(currentTab),
        [
          { opacity: 1, transform: 'translateY(0) scale(1)', filter: 'blur(0px)' },
          { opacity: 0, transform: 'translateY(12px) scale(0.99)', filter: 'blur(4px)' }
        ],
        { duration: 160, easing: 'cubic-bezier(0.4, 0, 1, 1)', fill: 'both' }
      );
    }

    setMainNavVisibility(tab);
    if (tab === 'community') loadCommunity();
    if (tab === 'discover') loadDiscover();
    if (tab === 'games-discover') loadGamesDiscover();
    if (tab === 'mylist' && viewingUser) await backToMyList();

    if (!prefersReducedMotion) {
      await animateMainNavPanels(
        getMainNavPanels(tab),
        [
          { opacity: 0, transform: 'translateY(14px) scale(0.995)', filter: 'blur(5px)' },
          { opacity: 1, transform: 'translateY(0) scale(1)', filter: 'blur(0px)' }
        ],
        { duration: 240, easing: 'cubic-bezier(0.16, 1, 0.3, 1)', fill: 'both' }
      );
      resetPanelStyles(getMainNavPanels(tab));
    }
  } catch(e) {
    console.error("Main nav switch failed:", e);
    setMainNavVisibility(tab);
    if (tab === 'community') loadCommunity();
    if (tab === 'discover') loadDiscover();
    if (tab === 'games-discover') loadGamesDiscover();
    if (tab === 'mylist' && viewingUser) await backToMyList();
  } finally {
    mainNavSwitching = false;
    persistUiState();
  }
}


let profileReturnTab = 'mylist';
let profileViewingUser = null;
let profileViewingProfile = null;
let profileViewingData = null;
let profileFavoriteSearchTimers = {};
let profileFavoriteSearchResults = {};
let profileFavoritePickerState = null;

const PROFILE_LINK_CONFIG = [
  { key: 'imdb', label: 'IMDb', domain: 'imdb.com', placeholder: 'https://www.imdb.com/user/...' },
  { key: 'letterboxd', label: 'Letterboxd', domain: 'letterboxd.com', placeholder: 'https://letterboxd.com/username/' },
  { key: 'backloggd', label: 'Backloggd', domain: 'backloggd.com', placeholder: 'https://www.backloggd.com/u/username/', optionalMobile: true, visibilityKey: 'linkBackloggd' },
  { key: 'instagram', label: 'Instagram', domain: 'instagram.com', placeholder: 'https://www.instagram.com/username/' },
  { key: 'twitter', label: 'Twitter / X', domain: 'x.com', placeholder: 'https://x.com/username' },
  { key: 'appleMusic', label: 'Apple Music', domain: 'music.apple.com', placeholder: 'https://music.apple.com/profile/username', optionalMobile: true, visibilityKey: 'linkAppleMusic' },
  { key: 'spotify', label: 'Spotify', domain: 'spotify.com', placeholder: 'https://open.spotify.com/user/username', optionalMobile: true, visibilityKey: 'linkSpotify' }
];

const PROFILE_MOBILE_LINK_CONFIG = [
  ...PROFILE_LINK_CONFIG
];

const PROFILE_DATABASE_FAVORITES = [
  { key: 'overallMedia', label: 'Top 3 Overall Media', shortLabel: 'Overall Media', icon: '🏆', optional: false, source: 'tmdb', tmdbType: 'multi', sourceLabel: 'TMDB', searchPlaceholder: 'Search TMDB movies, TV, or anime', ratingPlaceholder: 'Your rating' },
  { key: 'movies', section: 'movies', label: 'Top 3 Movies', shortLabel: 'Movies', icon: '🎬', optional: false, source: 'tmdb', tmdbType: 'movie', sourceLabel: 'TMDB', searchPlaceholder: 'Search TMDB movies', ratingPlaceholder: 'Your rating' },
  { key: 'shows', section: 'shows', label: 'Top 3 TV Shows', shortLabel: 'TV Shows', icon: '📺', optional: false, source: 'tmdb', tmdbType: 'tv', sourceLabel: 'TMDB', searchPlaceholder: 'Search TMDB TV shows', ratingPlaceholder: 'Your rating' },
  { key: 'anime', section: 'anime', label: 'Top 3 Animes', shortLabel: 'Anime', icon: '🌸', optional: true, source: 'tmdb', tmdbType: 'tv', sourceLabel: 'TMDB', searchPlaceholder: 'Search TMDB anime', ratingPlaceholder: 'Your rating' },
  { key: 'games', section: 'games', label: 'Top 3 Games', shortLabel: 'Games', icon: '🎮', optional: true, source: 'rawg', rawgType: 'game', sourceLabel: 'RAWG', searchPlaceholder: 'Search RAWG games', ratingPlaceholder: 'Your rating' },
  { key: 'singlePlayerGames', section: 'games', label: 'Top 3 Single Player Games', shortLabel: 'Single Player', icon: '🕹️', optional: true, source: 'rawg', rawgType: 'game', sourceLabel: 'RAWG', searchPlaceholder: 'Search RAWG single player games', ratingPlaceholder: 'Your rating' },
  { key: 'actors', label: 'Top 3 Actors / Actresses', shortLabel: 'Actors', icon: '🎭', optional: true, source: 'tmdb', tmdbType: 'person', sourceLabel: 'TMDB', searchPlaceholder: 'Search TMDB people', ratingPlaceholder: 'Why they rank for you' },
  { key: 'directors', label: 'Top 3 Directors', shortLabel: 'Directors', icon: '🎞️', optional: true, source: 'tmdb', tmdbType: 'person', sourceLabel: 'TMDB', searchPlaceholder: 'Search TMDB directors', ratingPlaceholder: 'Why they rank for you' }
];

const PROFILE_MANUAL_FAVORITES = [
  { key: 'fictionalCharacters', label: 'Top 3 Fictional Characters', shortLabel: 'Characters', icon: '🦸', optional: true, namePlaceholder: 'Character', ratingPlaceholder: 'Rating / note' },
  { key: 'musicArtists', label: 'Top 3 Musical Artists', shortLabel: 'Music Artists', icon: '🎵', optional: true, namePlaceholder: 'Artist', ratingPlaceholder: 'Rating / note' }
];

const PROFILE_MEDIA_GROUPS = [
  { key: 'overall', title: 'Overall Media', icon: '🏆', sub: 'Your top 3 across movies, TV shows, and anime. This row is always visible.', statKeys: [], rows: ['overallMedia'], wide: true },
  { key: 'movies', title: 'Movies', icon: '🎬', sub: 'Movie hours, average rating, and your top 3 movies.', statKeys: ['movieHours', 'movieAvg'], rows: ['movies'] },
  { key: 'shows', title: 'TV Shows', icon: '📺', sub: 'TV watch time, average rating, and your top 3 shows.', statKeys: ['tvHours', 'tvAvg'], rows: ['shows'] },
  { key: 'anime', title: 'Anime', icon: '🌸', sub: 'Anime watch time, rating, and optional top 3 anime.', statKeys: ['animeHours', 'animeAvg'], rows: ['anime'] },
  { key: 'games', title: 'Video Games', icon: '🎮', sub: 'Played hours, game rating, and optional top 3 games.', statKeys: ['gameHours', 'gamesAvg'], rows: ['games'] },
  { key: 'characters', title: 'Fictional Characters', icon: '🦸', sub: 'Optional top 3 characters that define your taste.', statKeys: [], rows: ['fictionalCharacters'], wide: true },
  { key: 'people', title: 'Actors & Directors', icon: '🎭', sub: 'Optional top 3 actors / actresses and directors.', statKeys: [], rows: ['actors', 'directors'], wide: true },
  { key: 'music', title: 'Music', icon: '🎵', sub: 'Optional top 3 musical artists.', statKeys: [], rows: ['musicArtists'], wide: true }
];

function getEmptyDatabaseFavorite() {
  return { id: '', source: '', type: '', title: '', image: '', rating: '', meta: '', legacyId: '' };
}

function getDefaultPinnedFavorites() {
  return PROFILE_DATABASE_FAVORITES.reduce((acc, group) => {
    acc[group.key] = [0, 1, 2].map(() => getEmptyDatabaseFavorite());
    return acc;
  }, {});
}

function getDefaultProfileVisibility() {
  return {
    anime: true,
    games: true,
    singlePlayerGames: true,
    fictionalCharacters: true,
    actors: true,
    directors: true,
    musicArtists: true,
    statsAnimeHours: true,
    statsGameHours: true,
    statsAnimeAvg: true,
    statsGamesAvg: true,
    linkBackloggd: true,
    linkAppleMusic: true,
    linkSpotify: true
  };
}

function getEmptyManualFavorite() { return { name: '', image: '', rating: '' }; }

function getDefaultShowcaseFavorites() {
  return PROFILE_MANUAL_FAVORITES.reduce((acc, group) => {
    acc[group.key] = [0,1,2].map(() => getEmptyManualFavorite());
    return acc;
  }, {});
}

function getDefaultSocialLinks() {
  return PROFILE_MOBILE_LINK_CONFIG.reduce((acc, link) => {
    acc[link.key] = '';
    return acc;
  }, {});
}

function isViewingOtherProfile() {
  return !!profileViewingUser;
}

function getActiveProfile() {
  return profileViewingProfile || userProfile || normalizeUserProfile({});
}

function getProfileFallbackPhotoFor(profile) {
  const name = profile?.name || currentUser?.displayName || 'ScreenList User';
  return 'https://ui-avatars.com/api/?name=' + encodeURIComponent(name) + '&background=1c1535&color=a78bfa';
}

function getViewingProfileName() {
  const profile = getActiveProfile();
  return profile?.name || profileViewingUser?.name || 'ScreenList User';
}

function normalizeDatabaseFavoriteEntry(entry) {
  const empty = getEmptyDatabaseFavorite();
  if (!entry) return empty;
  if (typeof entry === 'string') return { ...empty, legacyId: entry, id: entry, source: 'library' };
  return {
    id: String(entry.id || entry.tmdbId || entry.rawgId || entry.legacyId || '').trim(),
    source: String(entry.source || entry.db || entry.provider || '').trim(),
    type: String(entry.type || entry.mediaType || entry.tmdbType || '').trim(),
    title: String(entry.title || entry.name || '').trim(),
    image: String(entry.image || entry.cover || entry.poster || entry.photo || '').trim(),
    rating: String(entry.rating || entry.userRating || entry.note || '').trim(),
    meta: String(entry.meta || entry.year || entry.detail || '').trim(),
    legacyId: String(entry.legacyId || '').trim()
  };
}

function normalizePinnedFavorites(raw) {
  const defaults = getDefaultPinnedFavorites();
  const source = raw && typeof raw === 'object' ? raw : {};
  Object.keys(defaults).forEach(section => {
    const values = Array.isArray(source[section]) ? source[section] : [];
    defaults[section] = [0,1,2].map(i => normalizeDatabaseFavoriteEntry(values[i]));
  });
  return defaults;
}

function normalizeProfileVisibility(raw) {
  const defaults = getDefaultProfileVisibility();
  if (raw && typeof raw === 'object') {
    Object.keys(defaults).forEach(key => { defaults[key] = raw[key] !== false; });
  }
  return defaults;
}

function normalizeShowcaseFavorites(raw) {
  const defaults = getDefaultShowcaseFavorites();
  const source = raw && typeof raw === 'object' ? raw : {};
  Object.keys(defaults).forEach(section => {
    const values = Array.isArray(source[section]) ? source[section] : [];
    defaults[section] = [0,1,2].map(i => {
      const entry = values[i];
      if (!entry) return getEmptyManualFavorite();
      if (typeof entry === 'string') return { name: entry, image: '', rating: '' };
      return {
        name: String(entry.name || entry.title || '').trim(),
        image: String(entry.image || entry.photo || entry.cover || '').trim(),
        rating: String(entry.rating || entry.note || '').trim()
      };
    });
  });
  return defaults;
}

function normalizeSocialLinks(raw) {
  const links = getDefaultSocialLinks();
  if (raw && typeof raw === 'object') {
    Object.keys(links).forEach(key => { links[key] = String(raw[key] || '').trim(); });
  }
  return links;
}

function normalizeUserProfile(raw = {}) {
  const baseName = raw.name || raw.customName || (currentUser?.displayName) || 'ScreenList User';
  return {
    name: baseName,
    photo: raw.photo || raw.customPhoto || (currentUser?.photoURL) || '',
    bio: raw.bio || raw.profileBio || '',
    socialLinks: normalizeSocialLinks(raw.socialLinks),
    pinnedFavorites: normalizePinnedFavorites(raw.pinnedFavorites),
    profileVisibility: normalizeProfileVisibility(raw.profileVisibility),
    showcaseFavorites: normalizeShowcaseFavorites(raw.showcaseFavorites || raw.manualFavorites),
    uid: raw.uid || currentUser?.uid || 'preview-user',
    emailLower: raw.emailLower || raw.accountEmailLower || normalizeEmail(currentUser?.email)
  };
}

function getProfileDataForStats() {
  if (profileViewingData) return cloneListData(profileViewingData);
  if (!isViewingOtherProfile()) return cloneListData(ownDataCache || data || getEmptyListData());
  return cloneListData(getVisibleListData() || data || getEmptyListData());
}

function getWatchedEpisodeCount(item, section) {
  if (Array.isArray(item.episodes) && item.episodes.length) {
    const watched = item.episodes.filter(ep => ep && ep.watched).length;
    if (watched > 0) return watched;
  }
  const current = Number(item.currentEp || item.currentEpisode || 0);
  if (current > 0) return current;
  if (item.status === 'watched' || item.status === 'completed') return Number(item.totalEps || item.totalEpisodes || 0);
  return 0;
}

function calculateProfileStats() {
  const source = getProfileDataForStats();
  const movieHours = (source.movies || []).reduce((sum, item) => {
    if (item.status !== 'watched') return sum;
    const runtimeMinutes = Number(item.runtimeMinutes || item.runtime || 0);
    return sum + (runtimeMinutes > 0 ? runtimeMinutes / 60 : 2);
  }, 0);
  const tvHours = (source.shows || []).reduce((sum, item) => sum + (getWatchedEpisodeCount(item, 'shows') * 45 / 60), 0);
  const animeHours = (source.anime || []).reduce((sum, item) => sum + (getWatchedEpisodeCount(item, 'anime') * 24 / 60), 0);
  const gameHours = (source.games || []).reduce((sum, item) => {
    const explicit = Number(item.hoursPlayed || item.playtimeHours || 0);
    if (explicit > 0) return sum + explicit;
    const progress = Number(item.currentEp || item.currentHours || 0);
    return sum + Math.max(0, progress);
  }, 0);
  const avg = list => {
    const rated = list.filter(item => Number(item.rating || 0) > 0);
    if (!rated.length) return 'N/A';
    return (rated.reduce((sum, item) => sum + Number(item.rating || 0), 0) / rated.length).toFixed(1) + '/10';
  };
  const moviesTvHours = movieHours + tvHours;
  const allMediaHours = moviesTvHours + animeHours;
  const movieAvg = avg(source.movies || []);
  const tvAvg = avg(source.shows || []);
  const moviesTvAvg = avg([...(source.movies || []), ...(source.shows || [])]);
  const animeAvg = avg(source.anime || []);
  const gamesAvg = avg(source.games || []);
  const allMediaAvg = avg([...(source.movies || []), ...(source.shows || []), ...(source.anime || []), ...(source.games || [])]);
  return { movieHours, tvHours, moviesTvHours, allMediaHours, animeHours, gameHours, movieAvg, tvAvg, moviesTvAvg, animeAvg, gamesAvg, allMediaAvg };
}

function formatProfileHours(value) {
  const n = Number(value || 0);
  if (n <= 0) return '0h';
  if (n < 10 && n % 1) {
    return n.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + 'h';
  }
  return Math.round(n).toLocaleString('en-US') + 'h';
}

function renderProfileStats() {
  const el = document.getElementById('profile-stats-grid');
  if (!el) return;
  const stats = calculateProfileStats();
  const profile = getActiveProfile();
  const visibility = normalizeProfileVisibility(profile?.profileVisibility);
  const editing = !isViewingOtherProfile();
  const cards = [
    { key: 'allMediaHours', optional: false, tone: 'hours', value: formatProfileHours(stats.allMediaHours), labelMain: 'All Media', labelSub: 'Hours Watched' },
    { key: 'moviesTvHours', optional: false, tone: 'hours', value: formatProfileHours(stats.moviesTvHours), labelMain: 'TV Shows + Movies', labelSub: 'Hours Watched' },
    { key: 'statsAnimeHours', optional: true, tone: 'hours', value: formatProfileHours(stats.animeHours), labelMain: 'Anime', labelSub: 'Hours Watched' },
    { key: 'statsGameHours', optional: true, tone: 'hours', value: formatProfileHours(stats.gameHours), labelMain: 'Games', labelSub: 'Hours Played' },
    { key: 'allMediaAvg', optional: false, tone: 'score', value: stats.allMediaAvg, labelMain: 'Average Score', labelSub: 'All Media' },
    { key: 'moviesTvAvg', optional: false, tone: 'score', value: stats.moviesTvAvg, labelMain: 'Average Score', labelSub: 'TV Shows + Movies' },
    { key: 'statsAnimeAvg', optional: true, tone: 'score', value: stats.animeAvg, labelMain: 'Average Score', labelSub: 'Anime' },
    { key: 'statsGamesAvg', optional: true, tone: 'score', value: stats.gamesAvg, labelMain: 'Average Score', labelSub: 'Video Games' }
  ];
  el.innerHTML = cards.map(card => {
    const visible = !card.optional || visibility[card.key] !== false;
    if (!visible && !editing) return '';
    const toggle = card.optional && editing
      ? `<label class="profile-stat-toggle"><input type="checkbox" class="profile-section-toggle-input" data-profile-visible-key="${escAttr(card.key)}" ${visible ? 'checked' : ''} onchange="toggleProfileStatVisibility('${escAttr(card.key)}', this.checked)"> Display</label>`
      : '';
    return `
      <div class="profile-stat-card profile-stat-${escAttr(card.tone || 'default')} ${visible ? '' : 'profile-stat-hidden'}">
        ${toggle}
        <div class="profile-stat-value">${escHtml(visible ? card.value : 'Hidden')}</div>
        <div class="profile-stat-label"><span class="profile-stat-label-main">${escHtml(card.labelMain)}</span><span class="profile-stat-label-sub">${escHtml(card.labelSub)}</span></div>
      </div>
    `;
  }).join('');
}

function toggleProfileStatVisibility(key, checked) {
  if (isViewingOtherProfile()) return;
  if (!userProfile) userProfile = normalizeUserProfile({});
  readProfileDraftFromPage(userProfile);
  if (!userProfile.profileVisibility) userProfile.profileVisibility = getDefaultProfileVisibility();
  userProfile.profileVisibility[key] = checked !== false;
  renderProfileStats();
}

function getProfileItemById(section, id) {
  const source = getProfileDataForStats();
  return (source[section] || []).find(item => String(item.id) === String(id)) || null;
}

function getProfileFavoriteConfig(key) { return PROFILE_DATABASE_FAVORITES.find(group => group.key === key) || PROFILE_MANUAL_FAVORITES.find(group => group.key === key) || null; }
function getProfileStatLabel(key) {
  const map = {
    movieHours: 'Movies · Hours Watched',
    tvHours: 'TV Shows · Hours Watched',
    animeHours: 'Anime · Hours Watched',
    gameHours: 'Games · Hours Played',
    movieAvg: 'Average Score · Movies',
    tvAvg: 'Average Score · TV Shows',
    animeAvg: 'Average Score · Anime',
    gamesAvg: 'Average Score · Video Games'
  };
  return map[key] || '';
}
function getProfileStatValue(stats, key) { return key.endsWith('Hours') ? formatProfileHours(stats[key]) : (stats[key] || 'N/A'); }
function getProfileItemRating(item) {
  const rating = Number(item?.rating || 0);
  if (rating > 0) return '⭐ ' + rating.toFixed(rating % 1 ? 1 : 0) + '/10';
  return '';
}

function formatProfileFavoriteRatingDisplay(value, placeholder = 'Tap to rate') {
  const raw = String(value || '').trim();
  if (!raw) return '⭐ ' + placeholder;
  if (/^⭐/.test(raw)) return raw;
  if (/^★/.test(raw)) return raw.replace(/^★\s*/, '⭐ ');
  if (/^\d+(\.\d+)?\s*(\/\s*10)?$/.test(raw)) {
    const clean = raw.includes('/') ? raw.replace(/\s+/g, '') : raw + '/10';
    return '⭐ ' + clean;
  }
  return '⭐ ' + raw;
}

function normalizeProfileMatchText(value) {
  return String(value || '').toLowerCase().replace(/&/g, 'and').replace(/[^a-z0-9]+/g, ' ').trim();
}

function getProfileFavoriteCandidateSections(config, entry = {}) {
  if (config?.section) return [config.section];
  if (entry.type === 'movie') return ['movies'];
  if (entry.type === 'tv') return ['shows', 'anime'];
  if (entry.type === 'game') return ['games'];
  if (config?.key === 'overallMedia') return ['movies', 'shows', 'anime'];
  if (config?.source === 'rawg') return ['games'];
  return ['movies', 'shows', 'anime', 'games'];
}

function findMatchingLibraryItemForProfileFavorite(config, rawEntry) {
  const entry = normalizeDatabaseFavoriteEntry(rawEntry);
  const source = getProfileDataForStats();
  const candidateSections = getProfileFavoriteCandidateSections(config, entry);
  const titleKey = normalizeProfileMatchText(entry.title);
  for (const section of candidateSections) {
    const list = source[section] || [];
    const idMatch = list.find(item => {
      if (entry.source === 'tmdb' && entry.id && String(item.tmdbId || '') === String(entry.id)) return true;
      if (entry.source === 'rawg' && entry.id && String(item.rawgId || item.rawg_id || '') === String(entry.id)) return true;
      return false;
    });
    if (idMatch) return idMatch;
    if (titleKey) {
      const titleMatch = list.find(item => normalizeProfileMatchText(item.title) === titleKey);
      if (titleMatch) return titleMatch;
    }
  }
  return null;
}

function getProfileLibraryRatingForFavorite(config, rawEntry) {
  const item = findMatchingLibraryItemForProfileFavorite(config, rawEntry);
  return item ? getProfileItemRating(item) : '';
}
function isProfileRowVisible(key) {
  const profile = getActiveProfile();
  const config = getProfileFavoriteConfig(key);
  if (!config?.optional) return true;
  if (!profile.profileVisibility) profile.profileVisibility = getDefaultProfileVisibility();
  return profile.profileVisibility[key] !== false;
}
function renderProfileVisibilityToggle(key) {
  if (isViewingOtherProfile()) return '';
  const config = getProfileFavoriteConfig(key);
  if (!config?.optional) return '';
  const checked = isProfileRowVisible(key) ? 'checked' : '';
  return `<label class="profile-row-toggle"><input type="checkbox" class="profile-section-toggle-input" data-profile-visible-key="${escAttr(key)}" ${checked} onchange="toggleProfileRowVisibility('${escAttr(key)}', this.checked)"> Display</label>`;
}
function toggleProfileRowVisibility(key, checked) {
  if (isViewingOtherProfile()) return;
  if (!userProfile) userProfile = normalizeUserProfile({});
  readProfileDraftFromPage(userProfile);
  if (!userProfile.profileVisibility) userProfile.profileVisibility = getDefaultProfileVisibility();
  userProfile.profileVisibility[key] = checked !== false;
  renderProfileFavorites();
}

function getProfileDatabaseFavoriteDisplay(config, rawEntry) {
  const entry = normalizeDatabaseFavoriteEntry(rawEntry);
  const legacy = entry.legacyId || (entry.source === 'library' ? entry.id : '');
  if (legacy && config.section) {
    const item = getProfileItemById(config.section, legacy);
    if (item) {
      return {
        id: item.id || legacy,
        source: 'library',
        type: config.section,
        title: item.title || '',
        image: item.cover || '',
        rating: entry.rating || getProfileItemRating(item),
        meta: 'From library',
        legacyId: legacy
      };
    }
  }
  return { ...entry, rating: entry.rating || getProfileLibraryRatingForFavorite(config, entry) };
}

function getProfileDatabaseSearchLabel(config) {
  return config.source === 'rawg' ? 'RAWG' : 'TMDB';
}

function getProfileFavoriteCard(section, index) {
  return Array.from(document.querySelectorAll('.profile-db-slot')).find(card => card.dataset.profileDbSection === String(section) && card.dataset.profileDbIndex === String(index)) || null;
}

function ensureProfileFavoritePickerModal() {
  let overlay = document.getElementById('profile-favorite-picker-modal');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'profile-favorite-picker-modal';
    overlay.className = 'profile-favorite-picker-overlay';
    overlay.addEventListener('click', event => {
      if (event.target === overlay) closeProfileFavoritePicker();
    });
    document.body.appendChild(overlay);
  }
  return overlay;
}

function closeProfileFavoritePicker() {
  const overlay = document.getElementById('profile-favorite-picker-modal');
  if (overlay) overlay.classList.remove('open');
  profileFavoritePickerState = null;
}

function openProfileFavoritePicker(event, card) {
  if (isViewingOtherProfile() || !card) return;
  if (event) event.stopPropagation();
  if (card.classList.contains('profile-manual-slot')) {
    openProfileManualFavoritePicker(event, card);
    return;
  }
  const section = card.dataset.profileDbSection;
  const index = Number(card.dataset.profileDbIndex || 0);
  const config = getProfileFavoriteConfig(section);
  if (!config) return;
  profileFavoritePickerState = { mode: 'database', section, index, config, card, query: '', results: [], hit: null, rating: card.dataset.profileDbRating || '', libraryRating: '' };
  renderProfileFavoritePickerSearch();
}

function renderProfileFavoritePickerShell(inner) {
  const overlay = ensureProfileFavoritePickerModal();
  overlay.innerHTML = `<div class="profile-favorite-picker-modal" role="dialog" aria-modal="true">
    <div class="profile-picker-head">
      <div><div class="profile-picker-title">${escHtml(profileFavoritePickerState?.title || 'Choose Favorite')}</div><div class="profile-picker-sub">${escHtml(profileFavoritePickerState?.sub || '')}</div></div>
      <button type="button" class="profile-picker-close" onclick="closeProfileFavoritePicker()" aria-label="Close">×</button>
    </div>
    ${inner}
  </div>`;
  overlay.classList.add('open');
}

function renderProfileFavoritePickerSearch(message = '') {
  const state = profileFavoritePickerState;
  if (!state) return;
  state.title = state.config?.label || 'Choose Favorite';
  state.sub = 'Search title, select the result, then ScreenList will pull your existing library rating when it finds one.';
  const resultsHtml = message ? `<div class="profile-picker-message">${escHtml(message)}</div>` : '<div class="profile-picker-message">Search for the title you want to feature.</div>';
  renderProfileFavoritePickerShell(`
    <div class="profile-picker-searchbar">
      <input id="profile-picker-search-input" type="text" placeholder="Search title" value="${escAttr(state.query || '')}" onkeydown="if(event.key==='Enter'){event.preventDefault();profileFavoritePickerSearch();}">
      <button type="button" class="profile-picker-search-btn" onclick="profileFavoritePickerSearch()">Search</button>
    </div>
    <div id="profile-picker-results" class="profile-picker-results">${resultsHtml}</div>
    <div class="profile-picker-actions">
      ${state.card?.dataset.profileDbTitle ? '<button type="button" class="profile-picker-secondary-btn" onclick="clearProfileFavoriteFromPicker()">Clear</button>' : ''}
    </div>
  `);
  setTimeout(() => document.getElementById('profile-picker-search-input')?.focus(), 30);
}

async function profileFavoritePickerSearch() {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'database') return;
  const input = document.getElementById('profile-picker-search-input');
  const resultsEl = document.getElementById('profile-picker-results');
  const query = (input?.value || '').trim();
  state.query = query;
  if (!resultsEl) return;
  if (!query) {
    resultsEl.innerHTML = '<div class="profile-picker-message">Type a title first.</div>';
    return;
  }
  resultsEl.innerHTML = `<div class="profile-picker-message">Searching ${escHtml(getProfileDatabaseSearchLabel(state.config))}...</div>`;
  try {
    const hits = state.config.source === 'rawg' ? await searchProfileRawgFavorites(query) : await searchProfileTmdbFavorites(state.config, query);
    state.results = hits.slice(0, 8);
    if (!state.results.length) {
      resultsEl.innerHTML = '<div class="profile-picker-message">No results found.</div>';
      return;
    }
    resultsEl.innerHTML = state.results.map((hit, i) => {
      const thumb = hit.image ? `<img src="${escAttr(hit.image)}" alt="">` : `<span>${state.config.icon}</span>`;
      return `<button type="button" class="profile-picker-result" onclick="selectProfileFavoritePickerResult(${i})">
        <div class="profile-picker-result-img">${thumb}</div>
        <div class="profile-picker-result-copy"><strong>${escHtml(hit.title || 'Untitled')}</strong><span>${escHtml(hit.meta || getProfileDatabaseSearchLabel(state.config))}</span></div>
      </button>`;
    }).join('');
  } catch(e) {
    console.error('Profile favorite picker search failed:', e);
    resultsEl.innerHTML = '<div class="profile-picker-message">Search failed. Try again.</div>';
  }
}

function selectProfileFavoritePickerResult(resultIndex) {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'database') return;
  const hit = state.results?.[resultIndex];
  if (!hit) return;
  state.hit = hit;
  state.libraryRating = getProfileLibraryRatingForFavorite(state.config, hit);
  state.rating = state.libraryRating || '';
  renderProfileFavoritePickerConfirm();
}

function renderProfileFavoritePickerConfirm() {
  const state = profileFavoritePickerState;
  if (!state || !state.hit) return;
  const hit = state.hit;
  const coverStyle = hit.image ? `style="background-image:url('${escAttr(hit.image)}')"` : '';
  const libraryNote = state.libraryRating
    ? `<div class="profile-picker-library-note">Pulled from your library: ${escHtml(formatProfileFavoriteRatingDisplay(state.libraryRating, ''))}</div>`
    : '<div class="profile-picker-library-note">Not found in your library — add a rating to feature it.</div>';
  state.title = 'Confirm Favorite';
  state.sub = 'ScreenList scanned your library for this title before asking for a rating.';
  renderProfileFavoritePickerShell(`
    <div class="profile-picker-selected">
      <div class="profile-picker-selected-poster" ${coverStyle}>${hit.image ? '' : state.config.icon}</div>
      <div>
        <div class="profile-picker-selected-title">${escHtml(hit.title || 'Untitled')}</div>
        <div class="profile-picker-selected-meta">${escHtml(hit.meta || getProfileDatabaseSearchLabel(state.config))}</div>
        ${libraryNote}
      </div>
    </div>
    <input id="profile-picker-rating-input" class="profile-picker-rating-input" type="text" placeholder="Your rating" value="${escAttr(state.rating || '')}">
    <div class="profile-picker-actions">
      <button type="button" class="profile-picker-secondary-btn" onclick="renderProfileFavoritePickerSearch()">Back</button>
      <button type="button" class="profile-picker-confirm-btn" onclick="confirmProfileFavoritePicker()">Confirm</button>
    </div>
  `);
  if (!state.libraryRating) setTimeout(() => document.getElementById('profile-picker-rating-input')?.focus(), 30);
}

function writeProfileDatabaseFavoriteToCard(card, hit, rating) {
  if (!card || !hit) return;
  card.dataset.profileDbId = hit.id || '';
  card.dataset.profileDbSource = hit.source || '';
  card.dataset.profileDbType = hit.type || '';
  card.dataset.profileDbTitle = hit.title || '';
  card.dataset.profileDbImage = hit.image || '';
  card.dataset.profileDbMeta = hit.meta || '';
  card.dataset.profileDbLegacyId = '';
  card.dataset.profileDbRating = rating || '';
  updateProfileDatabaseCardPreview(card);
  if (userProfile) readProfileDraftFromPage(userProfile);
}

function confirmProfileFavoritePicker() {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'database' || !state.hit || !state.card) return;
  const rating = (document.getElementById('profile-picker-rating-input')?.value || state.rating || '').trim();
  if (!rating) {
    if (typeof showToast === 'function') showToast('Add a rating first');
    else alert('Add a rating first');
    return;
  }
  writeProfileDatabaseFavoriteToCard(state.card, state.hit, rating);
  closeProfileFavoritePicker();
}

function clearProfileFavoriteFromPicker() {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'database') return;
  clearProfileDatabaseFavorite(state.section, state.index);
  closeProfileFavoritePicker();
}

function updateProfileManualCardPreview(card) {
  if (!card) return;
  const section = card.dataset.manualSection;
  const config = getProfileFavoriteConfig(section) || { icon: '★' };
  const name = (card.dataset.manualName || '').trim();
  const image = (card.dataset.manualImage || '').trim();
  const rating = (card.dataset.manualRating || '').trim();
  const poster = card.querySelector('.profile-manual-preview');
  const namePreview = card.querySelector('[data-manual-name-preview]');
  const ratingPreview = card.querySelector('[data-manual-rating-preview]');
  if (poster) { poster.style.backgroundImage = image ? `url('${image.replace(/'/g, "%27")}')` : ''; poster.textContent = image ? '' : config.icon; }
  if (namePreview) { namePreview.textContent = name || 'Tap poster to add'; namePreview.classList.toggle('profile-fav-empty', !name); }
  if (ratingPreview) { ratingPreview.textContent = formatProfileFavoriteRatingDisplay(rating, 'Tap to rate'); ratingPreview.classList.toggle('profile-fav-empty-rating', !rating); }
}

function openProfileManualFavoritePicker(event, card) {
  if (isViewingOtherProfile() || !card) return;
  if (event) event.stopPropagation();
  const section = card.dataset.manualSection;
  const index = Number(card.dataset.manualIndex || 0);
  const config = getProfileFavoriteConfig(section) || { label: 'Top 3 Favorite', icon: '★', namePlaceholder: 'Name', ratingPlaceholder: 'Rating / note' };
  profileFavoritePickerState = { mode: 'manual', section, index, config, card, title: config.label, sub: 'Add the name, rating or note, and optional image for this profile spot.' };
  renderProfileManualFavoritePicker();
}

function renderProfileManualFavoritePicker() {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'manual') return;
  const card = state.card;
  renderProfileFavoritePickerShell(`
    <div class="profile-picker-manual-stack">
      <input id="profile-manual-picker-name" class="profile-picker-manual-input" type="text" placeholder="${escAttr(state.config.namePlaceholder || 'Name')}" value="${escAttr(card.dataset.manualName || '')}">
      <input id="profile-manual-picker-rating" class="profile-picker-manual-input" type="text" placeholder="${escAttr(state.config.ratingPlaceholder || 'Rating / note')}" value="${escAttr(card.dataset.manualRating || '')}">
      <input id="profile-manual-picker-image" class="profile-picker-manual-input" type="url" placeholder="Image URL" value="${escAttr(card.dataset.manualImage || '')}">
    </div>
    <div class="profile-picker-actions">
      ${card.dataset.manualName ? '<button type="button" class="profile-picker-secondary-btn" onclick="clearProfileManualFavoriteFromPicker()">Clear</button>' : ''}
      <button type="button" class="profile-picker-confirm-btn" onclick="confirmProfileManualFavoritePicker()">Confirm</button>
    </div>
  `);
  setTimeout(() => document.getElementById('profile-manual-picker-name')?.focus(), 30);
}

function confirmProfileManualFavoritePicker() {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'manual' || !state.card) return;
  state.card.dataset.manualName = (document.getElementById('profile-manual-picker-name')?.value || '').trim();
  state.card.dataset.manualRating = (document.getElementById('profile-manual-picker-rating')?.value || '').trim();
  state.card.dataset.manualImage = (document.getElementById('profile-manual-picker-image')?.value || '').trim();
  updateProfileManualCardPreview(state.card);
  if (userProfile) readProfileDraftFromPage(userProfile);
  closeProfileFavoritePicker();
}

function clearProfileManualFavoriteFromPicker() {
  const state = profileFavoritePickerState;
  if (!state || state.mode !== 'manual' || !state.card) return;
  state.card.dataset.manualName = '';
  state.card.dataset.manualRating = '';
  state.card.dataset.manualImage = '';
  updateProfileManualCardPreview(state.card);
  if (userProfile) readProfileDraftFromPage(userProfile);
  closeProfileFavoritePicker();
}

function toggleProfileFavoriteEditor(event, card) {
  openProfileFavoritePicker(event, card);
}

function updateProfileDatabaseCardPreview(card) {
  if (!card) return;
  const config = getProfileFavoriteConfig(card.dataset.profileDbSection) || { icon: '★', ratingPlaceholder: 'Your rating' };
  const title = (card.dataset.profileDbTitle || '').trim();
  const image = (card.dataset.profileDbImage || '').trim();
  const rating = (card.dataset.profileDbRating || card.querySelector('[data-profile-db-field="rating"]')?.value || '').trim();
  const poster = card.querySelector('.profile-fav-poster');
  const namePreview = card.querySelector('[data-db-name-preview]');
  const ratingPreview = card.querySelector('[data-db-rating-preview]');
  if (poster) { poster.style.backgroundImage = image ? `url('${image.replace(/'/g, "%27")}')` : ''; poster.textContent = image ? '' : config.icon; }
  if (namePreview) { namePreview.textContent = title || 'Tap poster to choose'; namePreview.classList.toggle('profile-fav-empty', !title); }
  if (ratingPreview) { ratingPreview.textContent = formatProfileFavoriteRatingDisplay(rating, 'Tap to rate'); ratingPreview.classList.toggle('profile-fav-empty-rating', !rating); }
}

function handleProfileDatabaseRatingInput(input) {
  updateProfileDatabaseCardPreview(input.closest('.profile-db-slot'));
  if (userProfile) readProfileDraftFromPage(userProfile);
}

function clearProfileDatabaseFavorite(section, index) {
  const card = getProfileFavoriteCard(section, index);
  if (!card) return;
  ['id', 'source', 'type', 'title', 'image', 'meta', 'legacyId', 'rating'].forEach(field => { card.dataset['profileDb' + field.charAt(0).toUpperCase() + field.slice(1)] = ''; });
  const search = card.querySelector('.profile-db-search-input');
  const rating = card.querySelector('[data-profile-db-field="rating"]');
  const results = card.querySelector('.profile-db-results');
  if (search) search.value = '';
  if (rating) rating.value = '';
  if (results) results.innerHTML = '';
  updateProfileDatabaseCardPreview(card);
  if (userProfile) readProfileDraftFromPage(userProfile);
}

function queueProfileDatabaseSearch(input) {
  const card = input.closest('.profile-db-slot');
  if (!card) return;
  const key = `${card.dataset.profileDbSection}-${card.dataset.profileDbIndex}`;
  clearTimeout(profileFavoriteSearchTimers[key]);
  profileFavoriteSearchTimers[key] = setTimeout(() => profileDatabaseFavoriteSearch(card.dataset.profileDbSection, Number(card.dataset.profileDbIndex || 0)), 420);
}

async function profileDatabaseFavoriteSearch(section, index) {
  const config = getProfileFavoriteConfig(section);
  const card = getProfileFavoriteCard(section, index);
  if (!config || !card) return;
  const input = card.querySelector('.profile-db-search-input');
  const results = card.querySelector('.profile-db-results');
  const query = (input?.value || '').trim();
  if (!results) return;
  if (!query) { results.innerHTML = ''; return; }
  results.innerHTML = `<div class="profile-db-message">Searching ${escHtml(getProfileDatabaseSearchLabel(config))}...</div>`;
  try {
    let hits = [];
    if (config.source === 'rawg') hits = await searchProfileRawgFavorites(query);
    else hits = await searchProfileTmdbFavorites(config, query);
    if (!hits.length) { results.innerHTML = '<div class="profile-db-message">No results found.</div>'; return; }
    results.innerHTML = hits.slice(0, 6).map((hit, i) => {
      const resultKey = `${section}-${index}-${Date.now()}-${i}`;
      profileFavoriteSearchResults[resultKey] = hit;
      const thumb = hit.image ? `<img src="${escAttr(hit.image)}" alt="">` : `<span>${config.icon}</span>`;
      return `<button type="button" class="profile-db-result" onclick="selectProfileDatabaseFavorite('${escAttr(section)}', ${index}, '${escAttr(resultKey)}')">
        <div class="profile-db-result-img">${thumb}</div>
        <div class="profile-db-result-copy"><strong>${escHtml(hit.title || 'Untitled')}</strong><span>${escHtml(hit.meta || getProfileDatabaseSearchLabel(config))}</span></div>
      </button>`;
    }).join('');
  } catch(e) {
    console.error('Profile favorite search failed:', e);
    results.innerHTML = '<div class="profile-db-message">Search failed. Try again.</div>';
  }
}

async function searchProfileTmdbFavorites(config, query) {
  const type = config.tmdbType || 'movie';
  const endpointType = type === 'multi' ? 'multi' : type;
  const res = await fetchTmdbProxy(`search/${endpointType}`, { query });
  if (!res.ok) throw new Error('TMDB favorite search failed');
  const json = await res.json();
  const results = (json.results || []).filter(item => {
    if (type === 'multi') return (item.media_type === 'movie' || item.media_type === 'tv') && item.poster_path;
    return type === 'person' ? item.profile_path : item.poster_path;
  }).slice(0, 8);
  return results.map(item => {
    const mediaType = type === 'multi' ? (item.media_type || 'movie') : type;
    const title = item.title || item.name || 'Untitled';
    const date = item.release_date || item.first_air_date || '';
    const year = date ? date.slice(0, 4) : '';
    const knownFor = (item.known_for || []).map(k => k.title || k.name).filter(Boolean).slice(0, 2).join(', ');
    const imagePath = mediaType === 'person' ? item.profile_path : item.poster_path;
    const mediaLabel = mediaType === 'tv' ? 'TV / Anime' : (mediaType === 'movie' ? 'Movie' : 'TMDB');
    return {
      id: String(item.id || ''),
      source: 'tmdb',
      type: mediaType,
      title,
      image: imagePath ? `https://image.tmdb.org/t/p/w500${imagePath}` : '',
      meta: mediaType === 'person' ? (knownFor || 'TMDB person') : [year, mediaLabel].filter(Boolean).join(' · '),
      rating: ''
    };
  });
}

async function searchProfileRawgFavorites(query) {
  const res = await fetchRawgProxy('games', { search: query, page_size: 8 });
  if (!res.ok) throw new Error('RAWG favorite search failed');
  const json = await res.json();
  return (json.results || []).filter(item => item.background_image).slice(0, 8).map(item => {
    const year = (item.released || '').slice(0, 4);
    const platforms = (item.platforms || []).map(p => p.platform?.name).filter(Boolean).slice(0, 2).join(', ');
    return {
      id: String(item.id || ''),
      source: 'rawg',
      type: 'game',
      title: item.name || 'Untitled',
      image: item.background_image || '',
      meta: [year, platforms].filter(Boolean).join(' · ') || 'RAWG game',
      rating: ''
    };
  });
}

function selectProfileDatabaseFavorite(section, index, resultKey) {
  const hit = profileFavoriteSearchResults[resultKey];
  const card = getProfileFavoriteCard(section, index);
  if (!hit || !card) return;
  card.dataset.profileDbId = hit.id || '';
  card.dataset.profileDbSource = hit.source || '';
  card.dataset.profileDbType = hit.type || '';
  card.dataset.profileDbTitle = hit.title || '';
  card.dataset.profileDbImage = hit.image || '';
  card.dataset.profileDbMeta = hit.meta || '';
  card.dataset.profileDbLegacyId = '';
  const search = card.querySelector('.profile-db-search-input');
  const ratingInput = card.querySelector('[data-profile-db-field="rating"]');
  const results = card.querySelector('.profile-db-results');
  const config = getProfileFavoriteConfig(section);
  const libraryRating = config ? getProfileLibraryRatingForFavorite(config, hit) : '';
  if (search) search.value = '';
  card.dataset.profileDbRating = libraryRating || '';
  if (ratingInput) {
    ratingInput.value = libraryRating || '';
    setTimeout(() => ratingInput.focus(), 40);
  }
  if (results) results.innerHTML = '';
  updateProfileDatabaseCardPreview(card);
  if (userProfile) readProfileDraftFromPage(userProfile);
}

function renderDatabaseFavoriteRow(key, pins) {
  const config = getProfileFavoriteConfig(key);
  if (!config) return '';
  const visible = isProfileRowVisible(key);
  const editing = !isViewingOtherProfile();
  const rowHead = `<div class="profile-fav-row-head"><div class="profile-fav-row-title">${escHtml(config.label)}</div>${renderProfileVisibilityToggle(key)}</div>`;
  if (!visible) return editing ? `<div class="profile-fav-row">${rowHead}<div class="profile-hidden-note">Hidden from profile. Toggle Display to show this row again.</div></div>` : '';
  const rankEmojis = ['🥇', '🥈', '🥉'];
  const slots = [0,1,2].map(i => {
    const entry = getProfileDatabaseFavoriteDisplay(config, pins[config.key]?.[i]);
    const title = entry.title || '';
    const image = entry.image || '';
    const rating = entry.rating || '';
    const cover = image ? `style="background-image:url('${escAttr(image)}')"` : '';
    const posterClick = editing ? `onclick="openProfileFavoritePicker(event, this.closest('.profile-fav-poster-card'))" title="Click to search title"` : '';
    return `<div class="profile-fav-poster-card profile-db-slot" data-profile-db-section="${escAttr(config.key)}" data-profile-db-index="${i}" data-profile-db-id="${escAttr(entry.id)}" data-profile-db-source="${escAttr(entry.source)}" data-profile-db-type="${escAttr(entry.type)}" data-profile-db-title="${escAttr(title)}" data-profile-db-image="${escAttr(image)}" data-profile-db-meta="${escAttr(entry.meta)}" data-profile-db-legacy-id="${escAttr(entry.legacyId)}" data-profile-db-rating="${escAttr(rating)}">
      <div class="profile-fav-poster ${editing ? 'profile-fav-poster-action' : ''}" ${cover} ${posterClick}>${image ? '' : config.icon}</div>
      <div class="profile-fav-name ${title ? '' : 'profile-fav-empty'}" data-db-name-preview>${escHtml(title || (editing ? 'Tap poster to choose' : 'Empty'))}</div>
      <div class="profile-fav-rating ${rating ? '' : 'profile-fav-empty-rating'}" data-db-rating-preview>${escHtml(formatProfileFavoriteRatingDisplay(rating, 'Tap to rate'))}</div>
      <div class="profile-fav-rank" aria-label="Rank ${i + 1}">${rankEmojis[i]}</div>
    </div>`;
  }).join('');
  return `<div class="profile-fav-row">${rowHead}<div class="profile-fav-poster-grid">${slots}</div></div>`;
}

function renderManualFavoriteRow(key, showcase) {
  const config = getProfileFavoriteConfig(key);
  if (!config) return '';
  const visible = isProfileRowVisible(key);
  const editing = !isViewingOtherProfile();
  const rowHead = `<div class="profile-fav-row-head"><div class="profile-fav-row-title">${escHtml(config.label)}</div>${renderProfileVisibilityToggle(key)}</div>`;
  if (!visible) return editing ? `<div class="profile-fav-row">${rowHead}<div class="profile-hidden-note">Hidden from profile. Toggle Display to show this row again.</div></div>` : '';
  const entries = showcase[key] || [0,1,2].map(() => getEmptyManualFavorite());
  const rankEmojis = ['🥇', '🥈', '🥉'];
  const slots = [0,1,2].map(i => {
    const entry = entries[i] || getEmptyManualFavorite();
    const cover = entry.image ? `style="background-image:url('${escAttr(entry.image)}')"` : '';
    const posterClick = editing ? `onclick="openProfileFavoritePicker(event, this.closest('.profile-fav-poster-card'))" title="Click to edit"` : '';
    return `<div class="profile-fav-poster-card profile-manual-slot" data-manual-section="${escAttr(key)}" data-manual-index="${i}" data-manual-name="${escAttr(entry.name)}" data-manual-image="${escAttr(entry.image)}" data-manual-rating="${escAttr(entry.rating)}">
      <div class="profile-fav-poster ${editing ? 'profile-fav-poster-action' : ''} profile-manual-preview" ${cover} ${posterClick}>${entry.image ? '' : config.icon}</div>
      <div class="profile-fav-name ${entry.name ? '' : 'profile-fav-empty'}" data-manual-name-preview>${escHtml(entry.name || (editing ? 'Tap poster to add' : 'Empty'))}</div>
      <div class="profile-fav-rating ${entry.rating ? '' : 'profile-fav-empty-rating'}" data-manual-rating-preview>${escHtml(formatProfileFavoriteRatingDisplay(entry.rating, 'Tap to rate'))}</div>
      <div class="profile-fav-rank" aria-label="Rank ${i + 1}">${rankEmojis[i]}</div>
    </div>`;
  }).join('');
  return `<div class="profile-fav-row">${rowHead}<div class="profile-fav-poster-grid">${slots}</div></div>`;
}

function renderProfileMediaGroup(group, stats, pins, showcase) {
  const statHtml = group.statKeys.length ? `<div class="profile-group-stats">${group.statKeys.map(key => `<div class="profile-group-stat"><div class="profile-group-stat-value">${escHtml(getProfileStatValue(stats, key))}</div><div class="profile-group-stat-label">${escHtml(getProfileStatLabel(key))}</div></div>`).join('')}</div>` : '';
  const rows = group.rows.map(rowKey => PROFILE_DATABASE_FAVORITES.some(item => item.key === rowKey) ? renderDatabaseFavoriteRow(rowKey, pins) : renderManualFavoriteRow(rowKey, showcase)).join('');
  return `<section class="profile-media-group ${group.wide ? 'profile-media-group-wide' : ''}" data-profile-group="${escAttr(group.key)}"><div class="profile-media-head"><div class="profile-media-title-wrap"><div class="profile-media-title"><span>${group.icon}</span><span>${escHtml(group.title)}</span></div><div class="profile-media-sub">${escHtml(group.sub)}</div></div></div>${statHtml}${rows}</section>`;
}
function readProfileDraftFromPage(target) {
  if (isViewingOtherProfile()) return target || getActiveProfile();
  const next = target || normalizeUserProfile(userProfile || {});
  next.profileVisibility = getDefaultProfileVisibility();
  document.querySelectorAll('.profile-section-toggle-input').forEach(input => {
    const key = input.dataset.profileVisibleKey;
    if (key && Object.prototype.hasOwnProperty.call(next.profileVisibility, key)) next.profileVisibility[key] = input.checked;
  });
  next.pinnedFavorites = normalizePinnedFavorites(next.pinnedFavorites);
  document.querySelectorAll('.profile-db-slot').forEach(card => {
    const section = card.dataset.profileDbSection;
    const index = Number(card.dataset.profileDbIndex || 0);
    if (!section || !next.pinnedFavorites[section] || !next.pinnedFavorites[section][index]) return;
    next.pinnedFavorites[section][index] = normalizeDatabaseFavoriteEntry({
      id: card.dataset.profileDbId || '',
      source: card.dataset.profileDbSource || '',
      type: card.dataset.profileDbType || '',
      title: card.dataset.profileDbTitle || '',
      image: card.dataset.profileDbImage || '',
      meta: card.dataset.profileDbMeta || '',
      legacyId: card.dataset.profileDbLegacyId || '',
      rating: (card.dataset.profileDbRating || card.querySelector('[data-profile-db-field="rating"]')?.value || '').trim()
    });
  });
  next.showcaseFavorites = normalizeShowcaseFavorites(next.showcaseFavorites);
  document.querySelectorAll('.profile-manual-slot').forEach(card => {
    const section = card.dataset.manualSection;
    const index = Number(card.dataset.manualIndex || 0);
    if (!section || !next.showcaseFavorites[section] || !next.showcaseFavorites[section][index]) return;
    next.showcaseFavorites[section][index] = {
      name: (card.dataset.manualName || '').trim(),
      image: (card.dataset.manualImage || '').trim(),
      rating: (card.dataset.manualRating || '').trim()
    };
  });
  return next;
}
function renderProfileFavoritesOnly() {
  if (!userProfile) userProfile = normalizeUserProfile({});
  readProfileDraftFromPage(userProfile);
  renderProfileFavorites();
}
function handleManualFavoriteInput(input) {
  const card = input.closest('.profile-manual-slot');
  if (!card) return;
  const nameInput = card.querySelector('[data-profile-manual-field="name"]');
  const ratingInput = card.querySelector('[data-profile-manual-field="rating"]');
  const imageInput = card.querySelector('[data-profile-manual-field="image"]');
  const namePreview = card.querySelector('[data-manual-name-preview]');
  const ratingPreview = card.querySelector('[data-manual-rating-preview]');
  const poster = card.querySelector('.profile-manual-preview');
  const section = input.dataset.profileManualSection;
  const config = getProfileFavoriteConfig(section) || { icon: '★', ratingPlaceholder: 'Rating / note' };
  if (namePreview) { const name = (nameInput?.value || '').trim(); namePreview.textContent = name || 'Tap poster to add'; namePreview.classList.toggle('profile-fav-empty', !name); }
  if (ratingPreview) { const rating = (ratingInput?.value || '').trim(); ratingPreview.textContent = formatProfileFavoriteRatingDisplay(rating, 'Tap to rate'); ratingPreview.classList.toggle('profile-fav-empty-rating', !rating); }
  if (poster) { const image = (imageInput?.value || '').trim(); poster.style.backgroundImage = image ? `url('${image.replace(/'/g, "%27")}')` : ''; poster.textContent = image ? '' : config.icon; }
}
function renderProfileFavorites() {
  const grid = document.getElementById('profile-favorites-grid');
  if (!grid) return;
  const profile = getActiveProfile();
  const pins = normalizePinnedFavorites(profile?.pinnedFavorites);
  const visibility = normalizeProfileVisibility(profile?.profileVisibility);
  const showcase = normalizeShowcaseFavorites(profile?.showcaseFavorites);
  profile.pinnedFavorites = pins; profile.profileVisibility = visibility; profile.showcaseFavorites = showcase;
  if (!isViewingOtherProfile()) userProfile = profile;
  else profileViewingProfile = profile;
  const stats = calculateProfileStats();
  grid.innerHTML = PROFILE_MEDIA_GROUPS.map(group => renderProfileMediaGroup(group, stats, pins, showcase)).join('');
}

function safeProfileUrl(value) {
  const url = String(value || '').trim();
  if (!url) return '';
  if (/^https?:\/\//i.test(url)) return url;
  return 'https://' + url.replace(/^\/+/, '');
}

function renderProfileLinks() {
  const grid = document.getElementById('profile-links-grid');
  if (!grid) return;
  const profile = getActiveProfile();
  const links = normalizeSocialLinks(profile?.socialLinks);
  const editing = !isViewingOtherProfile();
  grid.innerHTML = PROFILE_LINK_CONFIG.map(link => {
    const val = links[link.key] || '';
    const href = safeProfileUrl(val);
    const iconUrl = `https://www.google.com/s2/favicons?domain=${encodeURIComponent(link.domain)}&sz=64`;
    if (!editing) {
      return `<div class="profile-link-row">
        <div class="profile-link-icon"><img src="${iconUrl}" alt="${escAttr(link.label)}"></div>
        <div class="profile-link-field">
          <label>${escHtml(link.label)}</label>
          <div class="profile-readonly-empty-link">${href ? 'Linked profile' : 'Not linked'}</div>
        </div>
        <a class="profile-external-link ${href ? '' : 'disabled'}" href="${escAttr(href || '#')}" target="_blank" rel="noopener" title="Open ${escAttr(link.label)}">↗</a>
      </div>`;
    }
    return `<div class="profile-link-row">
      <div class="profile-link-icon"><img src="${iconUrl}" alt="${escAttr(link.label)}"></div>
      <div class="profile-link-field">
        <label>${escHtml(link.label)}</label>
        <input type="url" id="profile-link-${link.key}" placeholder="${escAttr(link.placeholder)}" value="${escAttr(val)}" oninput="renderProfileExternalLink('${link.key}')">
      </div>
      <a id="profile-open-${link.key}" class="profile-external-link ${href ? '' : 'disabled'}" href="${escAttr(href || '#')}" target="_blank" rel="noopener" title="Open ${escAttr(link.label)}">↗</a>
    </div>`;
  }).join('');
}

function renderProfileExternalLink(key) {
  const input = document.getElementById('profile-link-' + key);
  const link = document.getElementById('profile-open-' + key);
  if (!input || !link) return;
  const href = safeProfileUrl(input.value);
  link.href = href || '#';
  link.classList.toggle('disabled', !href);
}

function getProfileLinkConfig(key) {
  return PROFILE_MOBILE_LINK_CONFIG.find(link => link.key === key) || null;
}

function getProfileLinkIconUrl(link) {
  return `https://www.google.com/s2/favicons?domain=${encodeURIComponent(link.domain)}&sz=64`;
}

function renderProfileMobileLinks() {
  const grid = document.getElementById('profile-mobile-links-grid');
  if (!grid) return;
  const profile = getActiveProfile();
  const links = normalizeSocialLinks(profile?.socialLinks);
  const visibility = normalizeProfileVisibility(profile?.profileVisibility);
  const editing = !isViewingOtherProfile();
  const visibleLinks = PROFILE_MOBILE_LINK_CONFIG.map(link => {
    const visible = !link.optionalMobile || visibility[link.visibilityKey] !== false;
    if (!visible && !editing) return '';
    const href = safeProfileUrl(links[link.key] || '');
    const hiddenClass = visible ? '' : 'mobile-link-hidden';
    const emptyClass = href ? '' : 'empty';
    const toggle = link.optionalMobile && editing
      ? `<label class="profile-mobile-link-toggle" title="Show ${escAttr(link.label)} on profile"><input type="checkbox" class="profile-section-toggle-input" data-profile-visible-key="${escAttr(link.visibilityKey)}" ${visible ? 'checked' : ''} onchange="toggleProfileLinkVisibility('${escAttr(link.visibilityKey)}', this.checked)"></label>`
      : '';
    return `<div class="profile-mobile-link-wrap">
      ${toggle}
      <button type="button" class="profile-mobile-link-badge ${emptyClass} ${hiddenClass}" onclick="handleProfileMobileLinkClick(event, '${escAttr(link.key)}')" aria-label="${escAttr(link.label)} profile link" title="${escAttr(link.label)}">
        <img src="${escAttr(getProfileLinkIconUrl(link))}" alt="${escAttr(link.label)}">
      </button>
    </div>`;
  }).join('');
  const hint = editing ? '<div class="profile-mobile-links-hint">Tap an icon to edit or remove its link.</div>' : '';
  grid.innerHTML = visibleLinks + hint;
}

function handleProfileMobileLinkClick(event, key) {
  event.preventDefault();
  event.stopPropagation();
  const link = getProfileLinkConfig(key);
  if (!link) return;
  const profile = getActiveProfile();
  const href = safeProfileUrl(profile?.socialLinks?.[key] || '');

  if (!isViewingOtherProfile()) {
    openProfileLinkModal(event, key);
    return;
  }

  if (!href) {
    showToast(`${getViewingProfileName()} has not linked a profile for this yet`);
    return;
  }
  window.open(href, '_blank', 'noopener');
}

function openProfileLinkModal(event, key) {
  event.preventDefault();
  event.stopPropagation();
  if (isViewingOtherProfile()) return;
  const link = getProfileLinkConfig(key);
  if (!link) return;
  if (!userProfile) userProfile = normalizeUserProfile({});
  readProfileDraftFromPage(userProfile);
  userProfile.socialLinks = normalizeSocialLinks(userProfile.socialLinks);
  const current = userProfile.socialLinks[key] || '';
  const visibility = normalizeProfileVisibility(userProfile.profileVisibility);
  const isVisible = !link.optionalMobile || visibility[link.visibilityKey] !== false;

  const existing = document.getElementById('profile-link-edit-modal');
  if (existing) existing.remove();

  const toggleHtml = link.optionalMobile ? `
    <div class="plm-toggle-row">
      <span class="plm-toggle-label">Show on profile</span>
      <label class="plm-toggle-track">
        <input type="checkbox" id="plm-visibility" ${isVisible ? 'checked' : ''}>
        <span class="plm-toggle-thumb"></span>
      </label>
    </div>` : '';

  const modal = document.createElement('div');
  modal.id = 'profile-link-edit-modal';
  modal.className = 'plm-overlay';
  modal.innerHTML = `
    <div class="plm-sheet">
      <div class="plm-header">
        <img class="plm-icon" src="${escAttr(getProfileLinkIconUrl(link))}" alt="${escAttr(link.label)}">
        <span class="plm-title">${escHtml(link.label)}</span>
        <button class="plm-close" onclick="closeProfileLinkModal()">✕</button>
      </div>
      <input type="url" id="plm-url-input" class="plm-input" placeholder="${escAttr(link.placeholder)}" value="${escAttr(current)}">
      ${toggleHtml}
      <div class="plm-actions">
        <button class="plm-save-btn" onclick="saveProfileLinkModal('${escAttr(key)}')">Save</button>
        ${current ? `<button class="plm-remove-btn" onclick="removeProfileLinkModal('${escAttr(key)}')">Remove</button>` : ''}
      </div>
    </div>`;
  modal.addEventListener('click', e => { if (e.target === modal) closeProfileLinkModal(); });
  document.body.appendChild(modal);
  requestAnimationFrame(() => modal.classList.add('plm-open'));
  setTimeout(() => document.getElementById('plm-url-input')?.focus(), 50);
}

function closeProfileLinkModal() {
  const modal = document.getElementById('profile-link-edit-modal');
  if (!modal) return;
  modal.classList.remove('plm-open');
  setTimeout(() => modal.remove(), 230);
}

function saveProfileLinkModal(key) {
  const input = document.getElementById('plm-url-input');
  const visCheck = document.getElementById('plm-visibility');
  const link = getProfileLinkConfig(key);
  if (!input || !link) return;
  if (!userProfile) userProfile = normalizeUserProfile({});
  userProfile.socialLinks[key] = input.value.trim();
  if (visCheck && link.optionalMobile) {
    if (!userProfile.profileVisibility) userProfile.profileVisibility = getDefaultProfileVisibility();
    userProfile.profileVisibility[link.visibilityKey] = visCheck.checked;
  }
  closeProfileLinkModal();
  renderProfileLinks();
  renderProfileMobileLinks();
  showToast(input.value.trim() ? `${link.label} link updated` : `${link.label} link removed`);
}

function removeProfileLinkModal(key) {
  const link = getProfileLinkConfig(key);
  if (!link) return;
  if (!userProfile) userProfile = normalizeUserProfile({});
  userProfile.socialLinks[key] = '';
  closeProfileLinkModal();
  renderProfileLinks();
  renderProfileMobileLinks();
  showToast(`${link.label} link removed`);
}

function toggleProfileLinkVisibility(key, checked) {
  if (isViewingOtherProfile()) return;
  if (!userProfile) userProfile = normalizeUserProfile({});
  readProfileDraftFromPage(userProfile);
  if (!userProfile.profileVisibility) userProfile.profileVisibility = getDefaultProfileVisibility();
  userProfile.profileVisibility[key] = checked !== false;
  renderProfileMobileLinks();
}

function toggleProfilePhotoUrl() {
  const row = document.getElementById('profile-url-row');
  if (row) row.style.display = row.style.display === 'block' ? 'none' : 'block';
}

function getProfileFallbackPhoto() {
  return getProfileFallbackPhotoFor(getActiveProfile());
}

function renderProfilePage() {
  if (!userProfile) userProfile = normalizeUserProfile({});
  const profile = getActiveProfile();
  const viewingOther = isViewingOtherProfile();
  const profilePage = document.getElementById('profile-page');
  const titleEl = document.querySelector('.profile-topbar-title');
  const subEl = document.querySelector('.profile-topbar-sub');
  const saveBtn = document.querySelector('.profile-save-btn');
  const avatarActions = document.querySelector('.profile-avatar-actions');
  const themeRow = document.querySelector('.profile-theme-row');
  const nameInput = document.getElementById('profile-name');
  const photoInput = document.getElementById('profile-photo');
  const bioInput = document.getElementById('profile-bio');
  const fileInput = document.getElementById('profile-file');
  const urlRow = document.getElementById('profile-url-row');
  const preview = document.getElementById('profile-preview');
  const heroCard = document.querySelector('.profile-hero-card');
  let heroLogoutBtn = document.getElementById('profile-card-logout-btn');
  if (heroCard && !heroLogoutBtn) {
    heroCard.insertAdjacentHTML('afterbegin', '<button type="button" id="profile-card-logout-btn" class="profile-card-logout-btn" onclick="signOut()">Log out</button>');
    heroLogoutBtn = document.getElementById('profile-card-logout-btn');
  }
  if (profilePage) profilePage.classList.toggle('viewing-other-profile', viewingOther);
  if (titleEl) titleEl.textContent = viewingOther ? `${getViewingProfileName()}'s Profile` : 'Profile Studio';
  if (subEl) subEl.textContent = viewingOther ? 'Stats, favorites, linked profiles, and personal showcase' : 'Customize your ScreenList home page';
  if (saveBtn) saveBtn.style.display = viewingOther ? 'none' : '';
  if (heroLogoutBtn) heroLogoutBtn.style.display = viewingOther || isPreviewMode() ? 'none' : '';
  if (avatarActions) avatarActions.style.display = viewingOther ? 'none' : '';
  if (themeRow) themeRow.style.display = viewingOther ? 'none' : '';
  if (nameInput) {
    nameInput.value = profile.name || '';
    nameInput.readOnly = viewingOther;
    nameInput.setAttribute('aria-label', viewingOther ? 'Profile name' : 'Nickname');
  }
  if (photoInput) photoInput.value = profile.photo || '';
  if (bioInput) {
    bioInput.value = profile.bio || (viewingOther ? 'No bio yet.' : '');
    bioInput.readOnly = viewingOther;
  }
  if (fileInput) fileInput.value = '';
  if (urlRow) urlRow.style.display = 'none';
  if (preview) preview.src = profile.photo || getProfileFallbackPhotoFor(profile);
  const themeToggle = document.getElementById('theme-toggle-inp');
  if (themeToggle) themeToggle.checked = document.body.classList.contains('light-mode');
  renderProfileStats();
  renderProfileFavorites();
  renderProfileLinks();
  renderProfileMobileLinks();
}

function readProfileFromPage() {
  if (isViewingOtherProfile()) return getActiveProfile();
  const next = normalizeUserProfile(userProfile || {});
  next.name = (document.getElementById('profile-name')?.value || '').trim() || next.name || 'ScreenList User';
  next.photo = (document.getElementById('profile-photo')?.value || '').trim();
  next.bio = (document.getElementById('profile-bio')?.value || '').trim();
  next.socialLinks = normalizeSocialLinks(next.socialLinks || userProfile?.socialLinks || {});
  PROFILE_MOBILE_LINK_CONFIG.forEach(link => {
    const input = document.getElementById('profile-link-' + link.key);
    if (input) next.socialLinks[link.key] = (input.value || '').trim();
  });
  readProfileDraftFromPage(next);
  return next;
}

// Save user profile to Firestore
async function saveUserProfile(user) {
  try {
    const accountEmailLower = normalizeEmail(user?.email);
    const creatorAccount = accountEmailLower === CREATOR_ADMIN_EMAIL;
    const existing = await db.collection("users").doc(user.uid).get();
    const existingData = existing.exists ? existing.data() : {};
    const isNewUser = !existing.exists;
    const baseProfile = existingData.customName ? {
      ...existingData,
      name: existingData.customName,
      photo: existingData.customPhoto || existingData.photo || '',
      uid: user.uid,
      emailLower: accountEmailLower
    } : {
      ...existingData,
      name: creatorAccount ? CREATOR_DEFAULT_NAME : (user.displayName || 'Anonymous'),
      photo: user.photoURL || existingData.photo || '',
      uid: user.uid,
      emailLower: accountEmailLower
    };
    userProfile = normalizeUserProfile(baseProfile);
    if (isNewUser) {
      try {
        await db.collection("meta").doc("userCount").set({
          count: firebase.firestore.FieldValue.increment(1)
        }, { merge: true });
      } catch(e) { console.error("Counter increment failed:", e); }
    }
    await db.collection("users").doc(user.uid).set({
      name: userProfile.name,
      nameLower: (userProfile.name || '').trim().toLowerCase(),
      photo: userProfile.photo,
      customName: userProfile.name,
      customPhoto: userProfile.photo,
      bio: userProfile.bio || '',
      profileBio: userProfile.bio || '',
      socialLinks: userProfile.socialLinks || getDefaultSocialLinks(),
      pinnedFavorites: userProfile.pinnedFavorites || getDefaultPinnedFavorites(),
      profileVisibility: userProfile.profileVisibility || getDefaultProfileVisibility(),
      showcaseFavorites: userProfile.showcaseFavorites || getDefaultShowcaseFavorites(),
      emailLower: accountEmailLower,
      accountEmailLower: accountEmailLower,
      isCreatorAdmin: creatorAccount,
      isPublic: creatorAccount,
      uid: user.uid,
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    usersMap[user.uid] = {
      uid: user.uid,
      name: userProfile.name,
      photo: userProfile.photo,
      bio: userProfile.bio || '',
      socialLinks: userProfile.socialLinks || getDefaultSocialLinks(),
      pinnedFavorites: userProfile.pinnedFavorites || getDefaultPinnedFavorites(),
      profileVisibility: userProfile.profileVisibility || getDefaultProfileVisibility(),
      showcaseFavorites: userProfile.showcaseFavorites || getDefaultShowcaseFavorites(),
      emailLower: accountEmailLower,
      accountEmailLower: accountEmailLower,
      isCreatorAdmin: creatorAccount,
      isPublic: creatorAccount
    };
    applyProfile();
  } catch(e) { console.error("Profile save failed:", e); }
}

function applyProfile() {
  if (!userProfile) return;
  const avatar = document.getElementById("user-avatar");
  if (!avatar) return;
  avatar.src = userProfile.photo || getProfileFallbackPhoto();
  avatar.alt = userProfile.name || 'Profile';
  avatar.style.display = "block";
}

function buildPreviewProfileForUser(user) {
  const list = cloneListData(user?.listData || getEmptyListData());
  const firstEntry = section => (list[section] || []).filter(Boolean).slice(0, 3).map(item => normalizeDatabaseFavoriteEntry({
    id: item.tmdbId || item.rawgId || item.id || '',
    source: section === 'games' ? 'rawg' : 'tmdb',
    type: section === 'games' ? 'game' : section === 'movies' ? 'movie' : 'tv',
    title: item.title || '',
    image: item.cover || '',
    rating: getProfileItemRating(item),
    meta: item.genre || item.status || ''
  }));
  const fill = arr => [0, 1, 2].map(i => arr[i] || getEmptyDatabaseFavorite());
  return normalizeUserProfile({
    uid: user?.uid || 'preview-friend',
    name: user?.name || 'Preview User',
    photo: user?.photo || '',
    bio: user?.findStats || user?.stats || 'Preview community profile.',
    pinnedFavorites: {
      movies: fill(firstEntry('movies')),
      shows: fill(firstEntry('shows')),
      anime: fill(firstEntry('anime')),
      games: fill(firstEntry('games')),
      singlePlayerGames: fill(firstEntry('games')),
      actors: [0,1,2].map(() => getEmptyDatabaseFavorite()),
      directors: [0,1,2].map(() => getEmptyDatabaseFavorite())
    },
    profileVisibility: getDefaultProfileVisibility(),
    socialLinks: getDefaultSocialLinks(),
    showcaseFavorites: getDefaultShowcaseFavorites()
  });
}

function openPreviewUserProfile(uid) {
  const user = getPreviewCommunityUser(uid);
  if (!user) {
    showToast('Preview profile unavailable');
    return;
  }
  profileReturnTab = getActiveMainTab ? getActiveMainTab() : 'community';
  profileViewingUser = { uid: user.uid, name: user.name, photo: user.photo, preview: true };
  profileViewingProfile = buildPreviewProfileForUser(user);
  profileViewingData = cloneListData(user.listData || getEmptyListData());
  openProfilePageShell();
}

async function loadPublicProfileListData(uid) {
  try {
    const snap = await db.collection('watchlist').doc(uid).get();
    if (!snap.exists) return getEmptyListData();
    const d = snap.data();
    const loaded = {
      shows: d.shows ? JSON.parse(d.shows) : [],
      movies: d.movies ? JSON.parse(d.movies) : [],
      anime: d.anime ? JSON.parse(d.anime) : [],
      games: d.games ? JSON.parse(d.games) : []
    };
    return await autoSortAnimeBuckets(normalizeListData(loaded), false);
  } catch(e) {
    console.error('Failed to load profile list data:', e);
    return getEmptyListData();
  }
}

async function canViewUserProfile(uid) {
  if (!currentUser || !uid) return false;
  if (uid === currentUser.uid) return true;
  if (!friends.includes(uid)) return false;
  try {
    const userDoc = await db.collection('users').doc(uid).get();
    if (!userDoc.exists) return false;
    const u = userDoc.data() || {};
    usersMap[uid] = { ...u, uid: u.uid || uid };
    const theirFriends = Array.isArray(u.friends) ? u.friends : [];
    return theirFriends.includes(currentUser.uid);
  } catch(e) {
    console.error('Profile privacy check failed:', e);
    return false;
  }
}

async function openUserProfile(uid, name = '', photo = '') {
  if (isPreviewMode()) {
    openPreviewUserProfile(uid);
    return;
  }
  if (currentUser && uid === currentUser.uid) {
    openProfile();
    return;
  }
  const allowed = await canViewUserProfile(uid);
  if (!allowed) {
    showPrivateModal();
    return;
  }
  profileReturnTab = getActiveMainTab ? getActiveMainTab() : 'community';
  let profileDoc = null;
  try {
    profileDoc = await db.collection('users').doc(uid).get();
  } catch(e) {
    console.error('Failed to load profile:', e);
  }
  if (!profileDoc || !profileDoc.exists) {
    showToast('Could not load that profile');
    return;
  }
  const raw = { ...(profileDoc.data() || {}), uid };
  usersMap[uid] = raw;
  profileViewingUser = { uid, name: raw.name || name || 'Friend', photo: raw.photo || photo || '' };
  profileViewingProfile = normalizeUserProfile({ ...raw, uid, name: raw.name || name || 'Friend', photo: raw.photo || photo || '' });
  profileViewingData = (viewingUser?.uid === uid && friendViewData) ? cloneListData(friendViewData) : await loadPublicProfileListData(uid);
  openProfilePageShell();
}

function openProfilePageShell() {
  const mainNav = document.querySelector('.main-nav');
  const commentsPage = document.getElementById('comments-page');
  const activityPage = document.getElementById('activity-page');
  document.body.classList.add('profile-active');
  if (commentsPage) commentsPage.style.display = 'none';
  if (activityPage) activityPage.classList.remove('active');
  if (mainNav) mainNav.style.display = 'none';
  setMainNavVisibility('profile');
  renderProfilePage();
  const profilePage = document.getElementById('profile-page');
  if (profilePage) profilePage.scrollTo({ top: 0, behavior: 'auto' });
  window.scrollTo({ top: 0, behavior: 'auto' });
}

function openProfile() {
  profileViewingUser = null;
  profileViewingProfile = null;
  profileViewingData = null;
  if (!userProfile) userProfile = normalizeUserProfile({});
  profileReturnTab = getActiveMainTab ? getActiveMainTab() : 'mylist';
  openProfilePageShell();
}

function closeProfile() {
  const mainNav = document.querySelector('.main-nav');
  profileViewingUser = null;
  profileViewingProfile = null;
  profileViewingData = null;
  document.body.classList.remove('profile-active');
  if (mainNav) mainNav.style.display = 'flex';
  setMainNavVisibility(profileReturnTab || 'mylist');
  if (profileReturnTab === 'community') loadCommunity();
  if (profileReturnTab === 'discover') loadDiscover();
  if (profileReturnTab === 'games-discover') loadGamesDiscover();
  if ((profileReturnTab || 'mylist') === 'mylist') render();
  window.scrollTo({ top: 0, behavior: 'auto' });
}

function toggleTheme(isLight) {
  document.body.classList.toggle('light-mode', isLight);
  localStorage.setItem('theme', isLight ? 'light' : 'dark');
}

// Restore saved theme on load
(function() {
  if (localStorage.getItem('theme') === 'light') {
    document.body.classList.add('light-mode');
  }
})();

function previewProfilePhoto(url) {
  const preview = document.getElementById("profile-preview");
  if (url.trim()) {
    preview.src = url.trim();
    preview.onerror = () => { preview.src = 'https://ui-avatars.com/api/?name=?&background=1c1535&color=a78bfa'; };
  }
}

function handleProfileUpload(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {
    const img = new Image();
    img.onload = function() {
      const canvas = document.createElement('canvas');
      const size = 200;
      canvas.width = size;
      canvas.height = size;
      const ctx = canvas.getContext('2d');
      // Crop to square from center
      const min = Math.min(img.width, img.height);
      const sx = (img.width - min) / 2;
      const sy = (img.height - min) / 2;
      ctx.drawImage(img, sx, sy, min, min, 0, 0, size, size);
      const base64 = canvas.toDataURL('image/jpeg', 0.7);
      document.getElementById("profile-preview").src = base64;
      document.getElementById("profile-photo").value = base64;
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}

function getShareProfileUrl() {
  const url = new URL(window.location.href);
  url.hash = '';
  url.searchParams.delete('preview');
  const uid = profileViewingUser?.uid || profileViewingProfile?.uid || userProfile?.uid || currentUser?.uid || '';
  if (uid && uid !== 'preview-user') url.searchParams.set('profile', uid);
  else url.searchParams.delete('profile');
  return url.toString();
}

async function copyProfileLink(url) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(url);
      return true;
    }
  } catch (e) {}
  const input = document.createElement('input');
  input.value = url;
  input.setAttribute('readonly', '');
  input.style.position = 'fixed';
  input.style.opacity = '0';
  input.style.pointerEvents = 'none';
  document.body.appendChild(input);
  input.select();
  let copied = false;
  try { copied = document.execCommand('copy'); } catch (e) { copied = false; }
  input.remove();
  return copied;
}

async function shareProfile() {
  const activeProfile = isViewingOtherProfile() ? getActiveProfile() : readProfileFromPage();
  if (!isViewingOtherProfile()) userProfile = activeProfile;
  const shareUrl = getShareProfileUrl();
  const profileName = activeProfile?.name || currentUser?.displayName || 'ScreenList Profile';
  const shareData = {
    title: `${profileName} on ScreenList`,
    text: `Check out ${profileName}'s ScreenList profile.`,
    url: shareUrl
  };
  try {
    if (navigator.share) {
      await navigator.share(shareData);
      showToast('Profile share opened');
      return;
    }
  } catch (e) {
    if (e && e.name === 'AbortError') return;
  }
  const copied = await copyProfileLink(shareUrl);
  showToast(copied ? 'Profile link copied' : 'Could not copy profile link');
}

async function saveProfile() {
  if (isViewingOtherProfile()) { showToast('This is a read-only profile'); return; }
  const nextProfile = readProfileFromPage();
  userProfile = nextProfile;
  if (isPreviewMode() || !currentUser) {
    applyProfile();
    renderProfilePage();
    showToast("Preview profile updated");
    return;
  }
  const accountEmailLower = normalizeEmail(currentUser?.email);
  const creatorAccount = accountEmailLower === CREATOR_ADMIN_EMAIL;
  try {
    await db.collection("users").doc(currentUser.uid).set({
      name: nextProfile.name,
      nameLower: nextProfile.name.toLowerCase(),
      photo: nextProfile.photo,
      customName: nextProfile.name,
      customPhoto: nextProfile.photo,
      bio: nextProfile.bio || '',
      profileBio: nextProfile.bio || '',
      socialLinks: nextProfile.socialLinks || getDefaultSocialLinks(),
      pinnedFavorites: nextProfile.pinnedFavorites || getDefaultPinnedFavorites(),
      profileVisibility: nextProfile.profileVisibility || getDefaultProfileVisibility(),
      showcaseFavorites: nextProfile.showcaseFavorites || getDefaultShowcaseFavorites(),
      emailLower: accountEmailLower,
      accountEmailLower: accountEmailLower,
      isCreatorAdmin: creatorAccount,
      isPublic: creatorAccount,
      uid: currentUser.uid,
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    usersMap[currentUser.uid] = {
      uid: currentUser.uid,
      name: nextProfile.name,
      photo: nextProfile.photo,
      bio: nextProfile.bio || '',
      socialLinks: nextProfile.socialLinks || getDefaultSocialLinks(),
      pinnedFavorites: nextProfile.pinnedFavorites || getDefaultPinnedFavorites(),
      profileVisibility: nextProfile.profileVisibility || getDefaultProfileVisibility(),
      showcaseFavorites: nextProfile.showcaseFavorites || getDefaultShowcaseFavorites(),
      emailLower: accountEmailLower,
      accountEmailLower: accountEmailLower,
      isCreatorAdmin: creatorAccount,
      isPublic: creatorAccount
    };
  } catch(e) { console.error("Profile save failed:", e); }
  applyProfile();
  renderProfilePage();
  showToast("Profile updated");
}

// Load friends + requests from Firestore
function applyFriendsDataSnapshot(d = {}, opts = {}) {
  const prevIncomingRequests = incomingRequests.slice();
  const prevFriendsKey = friends.slice().sort().join('|');

  friends = Array.isArray(d.friends) ? d.friends : [];
  incomingRequests = Array.isArray(d.incomingRequests) ? d.incomingRequests : [];
  outgoingRequests = Array.isArray(d.outgoingRequests) ? d.outgoingRequests : [];

  updateRequestsBadges();
  updateFriendsCountBadge();

  if (prevFriendsKey !== friends.slice().sort().join('|')) {
    discoverFriendSocialCache = null;
    discoverFriendSocialCacheKey = '';
    discoverFriendSocialPromise = null;
    refreshDiscoverFriendStacks(true);
  }

  const communityActive = document.getElementById('nav-community')?.classList.contains('active');
  if (communityActive) {
    if (activeFriendsTab === 'requests') renderRequestsList();
    if (activeFriendsTab === 'friends') renderFriendsList();
    if (activeFriendsTab === 'find') refilterPeople();
  }

  if (!opts.silent) {
    const newRequests = incomingRequests.filter(uid => !prevIncomingRequests.includes(uid));
    if (newRequests.length > 0) {
      showToast(newRequests.length === 1 ? "New friend request" : `${newRequests.length} new friend requests`);
    }
  }
}

function stopFriendsDataListener() {
  if (friendsDataUnsubscribe) {
    friendsDataUnsubscribe();
    friendsDataUnsubscribe = null;
  }
  friendsDataLoadedOnce = false;
}

function resetFriendsDataState() {
  friends = [];
  incomingRequests = [];
  outgoingRequests = [];
  allUsersCache = [];
  usersMap = {};
  updateRequestsBadges();
  updateFriendsCountBadge();
}

function startFriendsDataListener() {
  if (!currentUser) return;
  stopFriendsDataListener();

  friendsDataUnsubscribe = db.collection("users").doc(currentUser.uid).onSnapshot(doc => {
    const d = doc.exists ? doc.data() : {};
    applyFriendsDataSnapshot(d, { silent: !friendsDataLoadedOnce });
    friendsDataLoadedOnce = true;
  }, e => {
    console.error("friends realtime listener failed:", e);
  });
}

async function loadFriendsData() {
  if (!currentUser) return;
  if (friendsDataLoadedOnce) {
    updateRequestsBadges();
    return;
  }

  try {
    const doc = await db.collection("users").doc(currentUser.uid).get();
    applyFriendsDataSnapshot(doc.exists ? doc.data() : {}, { silent: true });
  } catch(e) {
    console.error("loadFriendsData failed:", e);
    updateRequestsBadges();
  }
}

function updateRequestsBadges() {
  const tabBadge = document.getElementById('requests-count-badge');
  const navBtn = document.getElementById('nav-community');
  if (!tabBadge || !navBtn) return;
  if (incomingRequests.length > 0) {
    tabBadge.textContent = incomingRequests.length;
    tabBadge.style.display = 'inline-block';
    if (!navBtn.querySelector('.nav-badge')) {
      const b = document.createElement('span');
      b.className = 'nav-badge';
      navBtn.appendChild(b);
    }
    navBtn.querySelector('.nav-badge').textContent = incomingRequests.length;
  } else {
    tabBadge.style.display = 'none';
    const existing = navBtn.querySelector('.nav-badge');
    if (existing) existing.remove();
  }
}

// Entry point when switching to Friends nav
async function loadCommunity() {
  try {
    if (isPreviewMode()) {
      PREVIEW_COMMUNITY_USERS.forEach(user => { usersMap[user.uid] = user; });
      switchFriendsTab(activeFriendsTab || 'friends');
      return;
    }
    await loadFriendsData();
    switchFriendsTab(activeFriendsTab || 'friends');
  } catch(e) {
    console.error("loadCommunity failed:", e);
    const grid = document.getElementById('friends-grid');
    if (grid) grid.innerHTML = '<div class="app-error" style="grid-column:1/-1;">Friends could not load. Try again in a moment.</div>';
  }
}

function switchFriendsTab(tab) {
  activeFriendsTab = tab;
  const friendsTab = document.getElementById('ftab-friends');
  const requestsTab = document.getElementById('ftab-requests');
  const findTab = document.getElementById('ftab-find');
  const friendsView = document.getElementById('friends-list-view');
  const requestsView = document.getElementById('requests-view');
  const findView = document.getElementById('find-people-view');
  if (!friendsTab || !requestsTab || !findTab || !friendsView || !requestsView || !findView) {
    console.error("Community DOM is incomplete; cannot switch friends tab.");
    return;
  }
  friendsTab.classList.toggle('active', tab === 'friends');
  requestsTab.classList.toggle('active', tab === 'requests');
  findTab.classList.toggle('active', tab === 'find');
  friendsView.style.display = tab === 'friends' ? 'block' : 'none';
  requestsView.style.display = tab === 'requests' ? 'block' : 'none';
  findView.style.display = tab === 'find' ? 'block' : 'none';
  if (tab === 'friends') renderFriendsList();
  if (tab === 'requests') renderRequestsList();
  if (tab === 'find') initFindPeopleSearchView();
  persistUiState();
}

async function renderRequestsList() {
  const inGrid = document.getElementById('incoming-grid');
  const outGrid = document.getElementById('outgoing-grid');
  const inSec = document.getElementById('incoming-section');
  const outSec = document.getElementById('outgoing-section');
  if (!inGrid || !outGrid || !inSec || !outSec) {
    console.error("Requests DOM is incomplete.");
    return;
  }

  if (isPreviewMode()) {
    inSec.style.display = 'none';
    outSec.style.display = 'block';
    const heading = outSec.querySelector('h3');
    if (heading) heading.style.display = 'none';
    inGrid.innerHTML = '';
    outGrid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">📬</div>
      <p style="color:#7a6f99;font-size:14px;">Preview requests are simulated only</p>
      <p class="friends-empty-sub">Sign in to send, accept, and manage real friend requests.</p>
    </div>`;
    return;
  }

  if (incomingRequests.length === 0 && outgoingRequests.length === 0) {
    inSec.style.display = 'none';
    outSec.style.display = 'none';
    inGrid.innerHTML = '';
    outGrid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">📭</div>
      <p style="color:#7a6f99;font-size:14px;">No pending requests</p>
      <p class="friends-empty-sub">Friend invites and responses will appear here.</p>
    </div>`;
    outSec.style.display = 'block';
    document.getElementById('outgoing-section').querySelector('h3').style.display = 'none';
    return;
  }
  document.getElementById('outgoing-section').querySelector('h3').style.display = '';

  // Incoming
  if (incomingRequests.length > 0) {
    inSec.style.display = 'block';
    inGrid.innerHTML = '<div class="skeleton-card" style="grid-column:1/-1;"></div>';
    let docs = [];
    try {
      docs = await Promise.all(incomingRequests.map(uid => db.collection("users").doc(uid).get()));
    } catch(e) {
      console.error("Incoming requests load failed:", e);
      inGrid.innerHTML = '<div class="app-error" style="grid-column:1/-1;">Requests could not load. Try again in a moment.</div>';
      return;
    }
    let html = '';
    docs.forEach(doc => {
      if (!doc.exists) return;
      const u = doc.data();
      usersMap[u.uid] = u;
      const avatar = u.photo || 'https://ui-avatars.com/api/?name=' + encodeURIComponent(u.name || '?') + '&background=1e2028&color=60a5fa';
      html += `<div class="user-card locked" style="justify-content:space-between;">
        <div style="display:flex;align-items:center;gap:14px;flex:1;min-width:0;">
          <img class="user-card-avatar" src="${avatar}" alt="">
          <div><div class="user-card-name">${renderDisplayNameHTML(u, 'User')}</div><div class="user-card-stats">wants to be friends</div></div>
        </div>
        <div class="friend-actions-group">
          <button class="friend-action-btn friend-accept-btn" onclick="acceptFriendRequest('${u.uid}')">Accept</button>
          <button class="friend-action-btn friend-remove-btn" onclick="rejectFriendRequest('${u.uid}')">Decline</button>
        </div>
      </div>`;
    });
    inGrid.innerHTML = html;
  } else {
    inSec.style.display = 'none';
  }

  // Outgoing
  if (outgoingRequests.length > 0) {
    outSec.style.display = 'block';
    outGrid.innerHTML = '<div class="skeleton-card" style="grid-column:1/-1;"></div>';
    let docs = [];
    try {
      docs = await Promise.all(outgoingRequests.map(uid => db.collection("users").doc(uid).get()));
    } catch(e) {
      console.error("Outgoing requests load failed:", e);
      outGrid.innerHTML = '<div class="app-error" style="grid-column:1/-1;">Sent requests could not load. Try again in a moment.</div>';
      return;
    }
    let html = '';
    docs.forEach(doc => {
      if (!doc.exists) return;
      const u = doc.data();
      usersMap[u.uid] = u;
      const avatar = u.photo || 'https://ui-avatars.com/api/?name=' + encodeURIComponent(u.name || '?') + '&background=1e2028&color=60a5fa';
      html += `<div class="user-card locked" style="justify-content:space-between;">
        <div style="display:flex;align-items:center;gap:14px;flex:1;min-width:0;">
          <img class="user-card-avatar" src="${avatar}" alt="">
          <div><div class="user-card-name">${renderDisplayNameHTML(u, 'User')}</div><div class="user-card-stats">awaiting response</div></div>
        </div>
        <button class="friend-action-btn friend-remove-btn" onclick="cancelFriendRequest('${u.uid}')">Cancel</button>
      </div>`;
    });
    outGrid.innerHTML = html;
  } else {
    outSec.style.display = 'none';
  }
}

async function renderFriendsList() {
  const grid = document.getElementById('friends-grid');
  const badge = document.getElementById('friends-count-badge');
  if (!grid || !badge) {
    console.error("Friends DOM is incomplete.");
    return;
  }
  if (isPreviewMode()) {
    renderPreviewCommunityUsers(
      PREVIEW_COMMUNITY_USERS,
      'No preview friends available',
      'Preview friends help demonstrate the shared list experience.'
    );
    return;
  }
  if (friends.length === 0) {
    badge.textContent = '';
    grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">👥</div>
      <p style="color:#7a6f99;font-size:14px;">No friends yet</p>
      <p class="friends-empty-sub">Search for people you know and build a shared discovery shelf.</p>
    </div>`;
    return;
  }
  badge.textContent = '(' + friends.length + ')';
  grid.innerHTML = '<div class="skeleton-card" style="grid-column:1/-1;"></div>';
  try {
    const profiles = await Promise.all(friends.map(uid => db.collection("users").doc(uid).get()));
    let html = '';
    profiles.forEach(doc => {
      if (!doc.exists) return;
      const u = doc.data();
      usersMap[u.uid] = u;
      const avatar = u.photo || 'https://ui-avatars.com/api/?name=' + encodeURIComponent(u.name || '?') + '&background=1e2028&color=60a5fa';
      html += `<div class="user-card friend-list-card" style="justify-content:space-between;">
        <div class="friend-card-main" style="display:flex;align-items:center;gap:14px;flex:1;min-width:0;cursor:pointer;" onclick="viewUserFromMap('${u.uid}')">
          <img class="user-card-avatar" src="${avatar}" alt="">
          <div class="friend-card-copy"><div class="user-card-name">${renderDisplayNameHTML(u, 'User')}</div><div class="user-card-stats">Tap to view list</div></div>
        </div>
        <div class="friend-actions-group">
          <button class="friend-action-btn friend-mobile-list-btn" onclick="event.stopPropagation(); viewUserFromMap('${u.uid}')">Screen List</button>
          <button class="friend-action-btn friend-profile-btn friend-mobile-profile-btn" onclick="event.stopPropagation(); openUserProfile('${u.uid}')">Profile</button>
          <button class="friend-action-btn friend-profile-btn friend-profile-desktop-btn" onclick="event.stopPropagation(); openUserProfile('${u.uid}')">Profile</button>
          <button class="friend-action-btn friend-remove-btn friend-remove-desktop-btn" onclick="event.stopPropagation(); removeFriend('${u.uid}')">Remove</button>
          <button class="friend-mobile-remove-x" type="button" aria-label="Remove friend" onclick="event.stopPropagation(); confirmRemoveFriend('${u.uid}', '${escAttr(u.displayName || u.name || 'this friend')}')">×</button>
        </div>
      </div>`;
    });
    grid.innerHTML = html || `<div class="friends-empty" style="grid-column:1/-1;"><div class="friends-empty-icon">👥</div><p style="color:#7a6f99;">No friends found</p></div>`;
  } catch(e) {
    grid.innerHTML = '<div class="app-error" style="grid-column:1/-1;">Failed to load friends. Try again in a moment.</div>';
    console.error(e);
  }
}

async function loadAllUsers() {
  initFindPeopleSearchView();
  return;
  const grid = document.getElementById('all-users-grid');
  if (!grid) return;
  grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
    <div class="friends-empty-icon">👥</div>
    <p style="color:#7a6f99;font-size:14px;">Loading people...</p>
    <p class="friends-empty-sub">Everyone is visible. Full lists require mutual adds.</p>
  </div>`;

  if (allUsersCache.length === 0) {
    try {
      const snap = await db.collection("users").get();
      allUsersCache = [];
      snap.forEach(doc => {
        const u = doc.data();
        if (u.uid !== currentUser.uid) {
          allUsersCache.push(u);
          usersMap[u.uid] = u;
        }
      });
    } catch (e) {
      console.error("Failed to load users:", e);
      grid.innerHTML = '<div class="friends-empty" style="grid-column:1/-1;"><div class="friends-empty-icon">⚠️</div><p>Could not load users</p><p class="friends-empty-sub">Try again in a moment.</p></div>';
      return;
    }
  }

  renderAllUsers(allUsersCache);
}

function initFindPeopleSearchView() {
  const grid = document.getElementById('all-users-grid');
  if (!grid) return;
  if (isPreviewMode()) {
    grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">Search</div>
      <p style="color:#7a6f99;font-size:14px;">Search preview community members</p>
      <p class="friends-empty-sub">Type 2 or more characters to explore demo profiles. Friend actions stay preview-only.</p>
    </div>`;
    return;
  }
  grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
    <div class="friends-empty-icon">Search</div>
    <p style="color:#7a6f99;font-size:14px;">Search for the creator profile</p>
    <p class="friends-empty-sub">Only the public creator account appears here after you type the username.</p>
  </div>`;
}

async function searchUsersByUsername(query) {
  const normalized = (query || '').trim().toLowerCase();
  if (!normalized) {
    allUsersCache = [];
    initFindPeopleSearchView();
    return;
  }

  const grid = document.getElementById('all-users-grid');
  if (!grid) return;
  if (isPreviewMode()) {
    const previewMatches = PREVIEW_COMMUNITY_USERS.filter(user => (user.name || '').toLowerCase().includes(normalized));
    allUsersCache = previewMatches.slice();
    renderAllUsers(previewMatches, query);
    return;
  }
  grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
    <div class="friends-empty-icon">Search</div>
    <p style="color:#7a6f99;font-size:14px;">Searching...</p>
    <p class="friends-empty-sub">Looking for usernames matching "${escHtml(query)}".</p>
  </div>`;

  try {
    const snap = await db.collection("users")
      .where("nameLower", ">=", normalized)
      .where("nameLower", "<=", normalized + '\uf8ff')
      .limit(12)
      .get();

    allUsersCache = [];
    snap.forEach(doc => {
      const u = doc.data();
      if (u.uid !== currentUser.uid && shouldExposeInUserSearch(u)) {
        allUsersCache.push(u);
        usersMap[u.uid] = u;
      }
    });

    renderAllUsers(allUsersCache, query);
  } catch (e) {
    console.error("Failed to search users:", e);
    grid.innerHTML = '<div class="friends-empty" style="grid-column:1/-1;"><div class="friends-empty-icon">Error</div><p>Could not search users</p><p class="friends-empty-sub">Try again in a moment.</p></div>';
  }
}

function renderAllUsers(users, query = '') {
  const grid = document.getElementById('all-users-grid');
  if (!grid) return;
  if (isPreviewMode()) {
    if (!users || users.length === 0) {
      grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
        <div class="friends-empty-icon">🔍</div>
        <p>No preview users found for "${escHtml((query || '').trim())}"</p>
        <p class="friends-empty-sub">Try a different name or sign in to search the live community.</p>
      </div>`;
      return;
    }
    grid.innerHTML = users.map(user => `
      <div class="user-card" style="justify-content:space-between;">
        <div style="display:flex;align-items:center;gap:14px;flex:1;min-width:0;cursor:pointer;" onclick="openPreviewCommunityProfile('${user.uid}')">
          <img class="user-card-avatar" src="${user.photo}" alt="">
          <div style="min-width:0;">
            <div class="user-card-name">${renderDisplayNameHTML(user, 'Preview User')}</div>
            <div class="user-card-stats">${escHtml(user.findStats || user.stats || 'Preview profile')}</div>
          </div>
        </div>
        <div class="friend-actions-group">
          <button class="friend-action-btn friend-profile-btn" type="button" onclick="event.stopPropagation(); openPreviewUserProfile('${user.uid}')">Profile</button>
          <button class="friend-action-btn friend-pending-btn" type="button" disabled>Preview</button>
        </div>
      </div>
    `).join('');
    return;
  }
  const safeQuery = escHtml((query || '').trim());
  if (false && (!users || users.length === 0)) {
    grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">Search</div>
      <p>${safeQuery ? `No user found for "${safeQuery}"` : 'Search by username'}</p>
      <p class="friends-empty-sub">${safeQuery ? 'Try a different spelling or a shorter username.' : 'People only appear here after you search for a username.'}</p>
    </div>`;
    return;
  }

  if (!users || users.length === 0) {
    grid.innerHTML = '<div class="friends-empty" style="grid-column:1/-1;"><div class="friends-empty-icon">🔍</div><p>No other users found</p><p class="friends-empty-sub">Try a different name or spelling.</p></div>';
    return;
  }

  let html = '';
  users.forEach(u => {
    const isFriend = friends.includes(u.uid);
    const sentRequest = outgoingRequests.includes(u.uid);
    const receivedRequest = incomingRequests.includes(u.uid);
    const avatar = u.photo || ('https://ui-avatars.com/api/?name=' + encodeURIComponent(u.name || '?') + '&background=1e2028&color=60a5fa');

    let action = '';
    if (isFriend) {
      action = `<div class="friend-actions-group"><button class="friend-action-btn friend-profile-btn" onclick="event.stopPropagation(); openUserProfile('${u.uid}')">Profile</button><button class="friend-action-btn friend-remove-btn" onclick="event.stopPropagation(); removeFriend('${u.uid}')">Remove</button></div>`;
    } else if (sentRequest) {
      action = `<button class="friend-action-btn friend-pending-btn" onclick="event.stopPropagation(); cancelFriendRequest('${u.uid}')" title="Tap to cancel">Pending</button>`;
    } else if (receivedRequest) {
      action = `<button class="friend-action-btn friend-accept-btn" onclick="event.stopPropagation(); acceptFriendRequest('${u.uid}')">Accept</button>`;
    } else {
      action = `<button class="friend-action-btn friend-add-btn" onclick="event.stopPropagation(); sendFriendRequest('${u.uid}')">+ Add</button>`;
    }

    const statText = isFriend
      ? 'Tap to view full list'
      : 'Visible to everyone · full list requires mutual add';

    const cardStatText = isFriend ? 'Tap to view full list' : 'Public profile · found by username search';

      html += `
        <div class="user-card${(!isFriend && !isCreatorAdmin(u)) ? ' locked' : ''}" style="justify-content:space-between;">
          <div style="display:flex;align-items:center;gap:14px;flex:1;min-width:0;cursor:pointer;" onclick="viewUserFromMap('${u.uid}')">
            <img class="user-card-avatar" src="${avatar}" alt="">
            <div style="min-width:0;">
              <div class="user-card-name">${renderDisplayNameHTML(u, 'Unknown User')}</div>
              <div class="user-card-stats">${isFriend ? 'Tap to view full list' : 'Public creator profile · linked to the admin account'}</div>
            </div>
        </div>
        ${action}
      </div>`;
  });

  grid.innerHTML = html;
}

function viewUserFromMap(uid) {
  if (isPreviewMode()) {
    openPreviewCommunityProfile(uid);
    return;
  }
  const u = usersMap[uid];
  if (!u) {
    showToast("Could not open that profile");
    return;
  }
  viewUserList(uid, u.name, u.photo || '');
}

function showPrivateModal() {
  const modal = document.getElementById('private-modal');
  if (modal) modal.style.display = 'flex';
}
function closePrivateModal() {
  const modal = document.getElementById('private-modal');
  if (modal) modal.style.display = 'none';
}

function legacyFilterPeople(query) {
  const q = (query || '').toLowerCase().trim();
  const grid = document.getElementById('all-users-grid');
  if (!grid) return;
  if (!q) {
    renderAllUsers(allUsersCache);
    return;
  }
  const filtered = allUsersCache.filter(u => (u.name || '').toLowerCase().includes(q));
  if (filtered.length === 0) {
    grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">🤷</div>
      <p style="color:#7a6f99;font-size:14px;">No one found matching "${escHtml(query)}"</p>
      <p class="friends-empty-sub">Try a shorter name or different spelling.</p>
    </div>`;
    return;
  }
  renderAllUsers(filtered);
}

function filterPeople(query) {
  const q = (query || '').trim();
  const grid = document.getElementById('all-users-grid');
  if (!grid) return;
  if (!q) {
    allUsersCache = [];
    initFindPeopleSearchView();
    return;
  }
  if (q.length < 2) {
    grid.innerHTML = `<div class="friends-empty" style="grid-column:1/-1;">
      <div class="friends-empty-icon">Search</div>
      <p style="color:#7a6f99;font-size:14px;">Keep typing to search</p>
      <p class="friends-empty-sub">Enter at least 2 characters of the username.</p>
    </div>`;
    return;
  }
  clearTimeout(filterPeople._timer);
  filterPeople._timer = setTimeout(() => {
    searchUsersByUsername(q);
  }, 220);
}

// Re-run the current search (used after add/remove actions to refresh button states)
function refilterPeople() {
  const input = document.querySelector('#find-people-view .find-search');
  filterPeople(input ? input.value : '');
}

function updateFriendsCountBadge() {
  const badge = document.getElementById('friends-count-badge');
  if (badge) badge.textContent = friends.length ? '(' + friends.length + ')' : '';
}

// Send a friend request (one-sided until accepted)
async function sendFriendRequest(uid) {
  const targetUser = usersMap[uid];
  if (targetUser && !shouldExposeInUserSearch(targetUser)) {
    showToast("Only the creator account is available to add right now");
    return;
  }
  if (friends.includes(uid) || outgoingRequests.includes(uid)) return;
  if (incomingRequests.includes(uid)) { acceptFriendRequest(uid); return; }
  outgoingRequests.push(uid);
  allUsersCache = [];
  try {
    const arrayUnion = firebase.firestore.FieldValue.arrayUnion;
    await Promise.all([
      db.collection("users").doc(currentUser.uid).set({ outgoingRequests: arrayUnion(uid) }, { merge: true }),
      db.collection("users").doc(uid).set({ incomingRequests: arrayUnion(currentUser.uid) }, { merge: true })
    ]);
  } catch(e) {
    console.error("sendFriendRequest failed:", e);
    outgoingRequests = outgoingRequests.filter(f => f !== uid);
  }
  if (activeFriendsTab === 'find') refilterPeople();
  showToast("Friend request sent");
}

// Cancel a request I sent
async function cancelFriendRequest(uid) {
  outgoingRequests = outgoingRequests.filter(f => f !== uid);
  allUsersCache = [];
  try {
    const arrayRemove = firebase.firestore.FieldValue.arrayRemove;
    await Promise.all([
      db.collection("users").doc(currentUser.uid).set({ outgoingRequests: arrayRemove(uid) }, { merge: true }),
      db.collection("users").doc(uid).set({ incomingRequests: arrayRemove(currentUser.uid) }, { merge: true })
    ]);
  } catch(e) { console.error("cancelFriendRequest failed:", e); }
  if (activeFriendsTab === 'requests') renderRequestsList();
  if (activeFriendsTab === 'find') refilterPeople();
  showToast("Request canceled");
}

// Accept a request someone sent me — both become friends
async function acceptFriendRequest(uid) {
  incomingRequests = incomingRequests.filter(f => f !== uid);
  if (!friends.includes(uid)) friends.push(uid);
  allUsersCache = [];
  try {
    const arrayUnion = firebase.firestore.FieldValue.arrayUnion;
    const arrayRemove = firebase.firestore.FieldValue.arrayRemove;
    await Promise.all([
      db.collection("users").doc(currentUser.uid).set({
        incomingRequests: arrayRemove(uid),
        friends: arrayUnion(uid)
      }, { merge: true }),
      db.collection("users").doc(uid).set({
        outgoingRequests: arrayRemove(currentUser.uid),
        friends: arrayUnion(currentUser.uid)
      }, { merge: true })
    ]);
  } catch(e) { console.error("acceptFriendRequest failed:", e); }
  updateRequestsBadges();
  updateFriendsCountBadge();
  if (activeFriendsTab === 'requests') renderRequestsList();
  if (activeFriendsTab === 'friends') renderFriendsList();
  if (activeFriendsTab === 'find') refilterPeople();
  showToast("Friend added");
}

// Decline a request someone sent me
async function rejectFriendRequest(uid) {
  incomingRequests = incomingRequests.filter(f => f !== uid);
  try {
    const arrayRemove = firebase.firestore.FieldValue.arrayRemove;
    await Promise.all([
      db.collection("users").doc(currentUser.uid).set({ incomingRequests: arrayRemove(uid) }, { merge: true }),
      db.collection("users").doc(uid).set({ outgoingRequests: arrayRemove(currentUser.uid) }, { merge: true })
    ]);
  } catch(e) { console.error("rejectFriendRequest failed:", e); }
  updateRequestsBadges();
  if (activeFriendsTab === 'requests') renderRequestsList();
  showToast("Request declined");
}

function confirmRemoveFriend(uid, name) {
  const existing = document.getElementById('remove-friend-modal');
  if (existing) existing.remove();
  const modal = document.createElement('div');
  modal.id = 'remove-friend-modal';
  modal.className = 'plm-overlay';
  modal.innerHTML = `
    <div class="plm-sheet">
      <div class="plm-header">
        <span class="plm-title">Remove Friend</span>
        <button class="plm-close" onclick="closeRemoveFriendModal()">✕</button>
      </div>
      <p style="color:#a9a0c6;font-size:13px;line-height:1.5;">Remove <strong style="color:#f7f3ff;">${escHtml(name)}</strong> from your friends? They won't be notified.</p>
      <div class="plm-actions">
        <button class="plm-remove-btn" style="flex:1;" onclick="removeFriend('${escAttr(uid)}'); closeRemoveFriendModal();">Remove</button>
        <button class="plm-save-btn" onclick="closeRemoveFriendModal()">Cancel</button>
      </div>
    </div>`;
  modal.addEventListener('click', e => { if (e.target === modal) closeRemoveFriendModal(); });
  document.body.appendChild(modal);
  requestAnimationFrame(() => modal.classList.add('plm-open'));
}

function closeRemoveFriendModal() {
  const modal = document.getElementById('remove-friend-modal');
  if (!modal) return;
  modal.classList.remove('plm-open');
  setTimeout(() => modal.remove(), 230);
}

// Remove a confirmed friend (mutual)
async function removeFriend(uid) {
  friends = friends.filter(f => f !== uid);
  allUsersCache = [];
  try {
    const arrayRemove = firebase.firestore.FieldValue.arrayRemove;
    await Promise.all([
      db.collection("users").doc(currentUser.uid).set({ friends: arrayRemove(uid) }, { merge: true }),
      db.collection("users").doc(uid).set({ friends: arrayRemove(currentUser.uid) }, { merge: true })
    ]);
  } catch(e) { console.error("removeFriend failed:", e); }
  updateFriendsCountBadge();
  if (activeFriendsTab === 'friends') renderFriendsList();
  else if (activeFriendsTab === 'find') refilterPeople();
  showToast("Friend removed");
}

// View another user's list — only allowed if mutually friends
async function viewUserList(uid, name, photo) {
  if (isPreviewMode()) {
    openPreviewCommunityProfile(uid);
    return;
  }
  if (currentUser && uid === currentUser.uid) {
    switchMainNav('mylist');
    return;
  }
  const sourceUser = { ...(usersMap[uid] || {}), uid, name, photo };
  // Privacy: must be in my friends list
  if (!friends.includes(uid)) {
    showPrivateModal();
    return;
  }
  // Defense in depth: confirm they also have me as a friend
  try {
    const userDoc = await db.collection("users").doc(uid).get();
    const theirFriends = (userDoc.exists && userDoc.data().friends) || [];
    if (!theirFriends.includes(currentUser.uid)) {
      showPrivateModal();
      return;
    }
  } catch(e) {
    console.error("Privacy check failed:", e);
    return;
  }
  if (!viewingUser) {
    const freshOwnData = await loadOwnDataFromFirestore();
    myData = cloneListData(freshOwnData);
    data = cloneListData(freshOwnData);
    ownDataCache = cloneListData(freshOwnData);
  }
  viewingUser = sourceUser;
  let loadFailed = false;
  try {
    const snap = await db.collection("watchlist").doc(uid).get();
    if (snap.exists) {
      const d = snap.data();
      friendViewData = {
        shows: d.shows ? JSON.parse(d.shows) : [],
        movies: d.movies ? JSON.parse(d.movies) : [],
        anime: d.anime ? JSON.parse(d.anime) : [],
        games: d.games ? JSON.parse(d.games) : []
      };
    } else {
      friendViewData = getEmptyListData();
    }
  } catch(e) {
    console.error("Failed to load user list:", e);
    friendViewData = getEmptyListData();
    loadFailed = true;
  }
  friendViewData = await autoSortAnimeBuckets(normalizeListData(friendViewData), false);
  clearListSearch();
  const communityView = document.getElementById('community-view');
  const myListView = document.getElementById('mylist-view');
  const myListHeader = document.getElementById('mylist-header');
  const addBtn = document.getElementById('add-btn');
  const bannerArea = document.getElementById('viewing-banner-area');
  if (communityView) communityView.style.display = 'none';
  if (myListView) myListView.style.display = 'block';
  if (myListHeader) myListHeader.style.display = 'block';
  if (addBtn) addBtn.style.display = 'none';
  if (bannerArea) bannerArea.innerHTML = `<div class="viewing-banner">
    <span class="viewing-banner-text">
      <img src="${photo || 'https://ui-avatars.com/api/?name=' + encodeURIComponent(name) + '&background=1e2028&color=60a5fa'}">
      Viewing <span class="viewing-banner-name">${renderDisplayNameHTML(sourceUser, 'Friend', 'creator-name-soft')}</span>'s list
    </span>
    <div class="viewing-banner-actions">
      <button class="back-btn profile-view-btn" onclick="openUserProfile('${uid}')">View Profile</button>
      <button class="back-btn" onclick="backToMyList()">← Back to My List</button>
    </div>
  </div>`;
  const initialView = chooseInitialListView(friendViewData);
  activeSection = initialView.section;
  activeTab = initialView.tab;
  render();
  persistUiState();
  if (loadFailed) {
    const grid = document.getElementById('cards-grid');
    const empty = document.getElementById('empty-state');
    if (empty) empty.style.display = 'none';
    if (grid) grid.innerHTML = '<div class="app-error" style="grid-column:1/-1;">This list could not load. Try again in a moment.</div>';
  }
}

async function backToMyList() {
  const previousFriendData = friendViewData ? cloneListData(friendViewData) : null;
  viewingUser = null;
  friendViewData = null;

  const addBtn = document.getElementById('add-btn');
  const bannerArea = document.getElementById('viewing-banner-area');
  const mainNav = document.querySelector('.main-nav');
  const navMyList = document.getElementById('nav-mylist');
  const navCommunity = document.getElementById('nav-community');
  const navDiscover = document.getElementById('nav-discover');
  const navGamesDiscover = document.getElementById('nav-games-discover');

  if (addBtn) addBtn.style.display = '';
  if (bannerArea) bannerArea.innerHTML = '';
  if (mainNav) mainNav.style.display = 'flex';
  if (navMyList) navMyList.classList.add('active');
  if (navCommunity) navCommunity.classList.remove('active');
  if (navDiscover) navDiscover.classList.remove('active');
  if (navGamesDiscover) navGamesDiscover.classList.remove('active');
  setMainNavVisibility('mylist');

  if (isPreviewMode()) {
    const previewOwnData = ownDataCache ? cloneListData(ownDataCache) : cloneListData(DEMO_DATA);
    activeSection = "shows";
    activeTab = "watching";
    clearListSearch();
    data = cloneListData(previewOwnData);
    render();
    persistUiState();
    return;
  }

  let freshOwnData = await loadOwnDataFromFirestore();
  if (previousFriendData && isSameListData(freshOwnData, previousFriendData)) {
    const backup = readOwnLocalBackup(previousFriendData);
    if (backup) {
      freshOwnData = await writeOwnDataDirect(backup);
      showToast("Restored your library");
    }
  }
  freshOwnData = await autoSortAnimeBuckets(freshOwnData, true);

  activeSection = "shows";
  activeTab = "watching";
  clearListSearch();

  data = cloneListData(freshOwnData);
  ownDataCache = cloneListData(freshOwnData);
  myData = null;
  if (currentUser) localStorage.setItem("screenlist-own-data-backup-" + currentUser.uid, JSON.stringify(freshOwnData));
  render();
  persistUiState();
}

// ===== Comments Page =====
let commentsItemId = null;
let commentsMediaKey = null;
let commentsUnsubscribe = null;
let commentsScope = 'friends';
let commentsRawItems = [];
let commentsDrafts = { friends: '', global: '' };
let commentsSubmitting = false;
let commentCountCache = {};

function getMediaKey(item) {
  if (!item) return '';
  if (item.imdbId) return 'imdb:' + item.imdbId;
  if (item.tmdbId) {
    const tmdbType = isShowSection(item.librarySection || item.mediaCategory || activeSection) ? 'tv' : 'movie';
    return `tmdb-${tmdbType}:${item.tmdbId}`;
  }
  if (item.metacriticSlug) return 'game:' + item.metacriticSlug;
  const title = (item.title || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  const type = item.librarySection || item.mediaCategory || activeSection || 'media';
  return type + ':' + title;
}

function getCachedCommentCount(mediaKey) {
  if (!mediaKey) return 0;
  return Number(commentCountCache[mediaKey] || 0);
}

function setCachedCommentCount(mediaKey, count) {
  if (!mediaKey) return;
  commentCountCache[mediaKey] = Math.max(0, Number(count) || 0);
}

function updateCommentCountBadges(mediaKey, count) {
  if (!mediaKey) return;
  document.querySelectorAll(`.comment-count[data-media-key="${CSS.escape(mediaKey)}"]`).forEach(el => {
    el.textContent = String(Math.max(0, Number(count) || 0));
  });
}

async function refreshVisibleCommentCounts() {
  const badges = Array.from(document.querySelectorAll('.comment-count[data-media-key]'));
  const uniqueKeys = Array.from(new Set(badges.map(el => el.dataset.mediaKey).filter(Boolean)));
  if (!uniqueKeys.length) return;

  await Promise.all(uniqueKeys.map(async mediaKey => {
    if (isPreviewMode() && !currentUser) {
      const previewCount = getPreviewCommentsForMedia(mediaKey).length;
      setCachedCommentCount(mediaKey, previewCount);
      updateCommentCountBadges(mediaKey, previewCount);
      return;
    }
    try {
      const snap = await db.collection('comments').doc(mediaKey).get();
      const comments = snap.exists && Array.isArray(snap.data().comments) ? snap.data().comments : [];
      setCachedCommentCount(mediaKey, comments.length);
      updateCommentCountBadges(mediaKey, comments.length);
    } catch (error) {
      console.error('Comment count load failed:', error);
      setCachedCommentCount(mediaKey, 0);
      updateCommentCountBadges(mediaKey, 0);
    }
  }));
}

function isFriendVisibleComment(comment) {
  if (!comment?.uid || !currentUser) return false;
  return comment.uid === currentUser.uid || friends.includes(comment.uid);
}

function getScopedComments(comments, scope) {
  const list = Array.isArray(comments) ? comments : [];
  if (scope === 'global') {
    return list.filter(comment => (comment.scope || 'global') !== 'friends');
  }
  if (!currentUser) return [];
  return list.filter(comment => (comment.scope || 'global') === 'friends' && isFriendVisibleComment(comment));
}

function getCommentsEmptyMessage(scope) {
  if (scope === 'friends') {
    if (!currentUser) return 'Sign in to see friends-only comments.';
    return 'No friends-only comments yet. Start the conversation with your friends.';
  }
  if (isPreviewMode() && !currentUser) return 'No preview global comments yet for this title.';
  return 'No global comments yet. Be the first to say something.';
}

function renderCommentsToolbar() {
  const countEl = document.getElementById('comments-count');
  if (!countEl) return;
  const filtered = getScopedComments(commentsRawItems, commentsScope);
  countEl.innerHTML = `<div class="comments-count">${filtered.length} Comment${filtered.length !== 1 ? 's' : ''}</div>`;
}

function renderCommentsInput() {
  const area = document.getElementById('comments-input-area');
  if (!area) return;
  if (!currentUser) {
    if (isPreviewMode()) {
      const note = commentsScope === 'friends'
        ? 'Friends-only comments are visible after sign-in.'
        : 'Preview Mode lets you read public comments, but nothing can be posted or saved.';
      area.innerHTML = `
        <div class="comment-input-area">
          <div class="comment-input-right">
            <div class="comment-input-footer">
              <div class="comment-input-left">
                <div class="comments-scope-tabs">
                  <button type="button" class="comments-scope-tab${commentsScope === 'friends' ? ' active' : ''}" onclick="switchCommentsScope('friends')">Friends</button>
                  <button type="button" class="comments-scope-tab${commentsScope === 'global' ? ' active' : ''}" onclick="switchCommentsScope('global')">Global</button>
                </div>
                <div class="comments-scope-note">${note}</div>
              </div>
            </div>
          </div>
        </div>`;
      return;
    }
    area.innerHTML = '';
    return;
  }
  const photo = (userProfile && userProfile.photo) || currentUser.photoURL || `https://ui-avatars.com/api/?name=${encodeURIComponent((userProfile && userProfile.name) || currentUser.displayName || '?')}&background=1e2028&color=60a5fa`;
  const placeholder = commentsScope === 'friends'
    ? 'Write a friends-only comment...'
    : 'Write a global comment...';
  const buttonLabel = commentsScope === 'friends' ? 'Post to Friends' : 'Post Globally';
  const note = commentsScope === 'friends'
    ? 'Only you and confirmed friends can see these comments.'
    : 'Anyone who opens this media can see these comments.';
  const draft = commentsDrafts[commentsScope] || '';
  area.innerHTML = `
    <div class="comment-input-area">
      <img class="comment-input-avatar" src="${photo}" alt="">
      <div class="comment-input-right">
        <textarea class="comment-textarea" id="comment-textarea" placeholder="${placeholder}" oninput="cacheCommentDraft(this.value)">${escHtml(draft)}</textarea>
        <div class="comment-input-footer">
          <div class="comment-input-left">
            <div class="comments-scope-tabs">
              <button type="button" class="comments-scope-tab${commentsScope === 'friends' ? ' active' : ''}" onclick="switchCommentsScope('friends')">Friends</button>
              <button type="button" class="comments-scope-tab${commentsScope === 'global' ? ' active' : ''}" onclick="switchCommentsScope('global')">Global</button>
            </div>
            <div class="comments-scope-note">${note}</div>
          </div>
          <button type="button" class="comment-post-btn" onclick="postComment()"${commentsSubmitting ? ' disabled' : ''}>${buttonLabel}</button>
        </div>
      </div>
    </div>`;
}

function cacheCommentDraft(value) {
  commentsDrafts[commentsScope] = value || '';
}

function switchCommentsScope(scope) {
  const input = document.getElementById('comment-textarea');
  if (input) commentsDrafts[commentsScope] = input.value || '';
  commentsScope = scope === 'global' ? 'global' : 'friends';
  renderCommentsToolbar();
  renderCommentsInput();
  renderCommentsUI(commentsRawItems);
}

function dismissCommentsPageForProfileNavigation() {
  if (commentsUnsubscribe) {
    commentsUnsubscribe();
    commentsUnsubscribe = null;
  }
  const commentsPageEl = document.getElementById('comments-page');
  if (commentsPageEl) {
    commentsPageEl.classList.remove('comments-page-animating', 'comments-page-animating-in', 'comments-page-closing');
    commentsPageEl.style.display = 'none';
    commentsPageEl.style.position = '';
    commentsPageEl.style.left = '';
    commentsPageEl.style.top = '';
    commentsPageEl.style.width = '';
    commentsPageEl.style.height = '';
    commentsPageEl.style.zIndex = '';
    commentsPageEl.style.overflowY = '';
    commentsPageEl.style.opacity = '';
    commentsPageEl.style.transform = '';
    commentsPageEl.style.pointerEvents = '';
    commentsPageEl.style.removeProperty('--comments-origin-x');
    commentsPageEl.style.removeProperty('--comments-origin-y');
  }
  commentsItemId = null;
  commentsMediaKey = null;
  commentsScope = 'friends';
  commentsRawItems = [];
  commentsDrafts = { friends: '', global: '' };
  commentsTransitionOrigin = null;
  commentsTransitionOriginRect = null;
  commentsPageClosing = false;
  commentsCloseAnimation = null;
  commentsRestoreView = null;
  const mainNav = document.querySelector('.main-nav');
  if (mainNav) mainNav.style.display = 'flex';
}

async function openCommentAuthorProfile(commentId) {
  const comment = commentsRawItems.find(entry => entry.id === commentId);
  if (!comment) return;

  dismissCommentsPageForProfileNavigation();

  if (isPreviewMode() || getPreviewCommunityUser(comment.uid)) {
    openPreviewCommunityProfile(comment.uid);
    return;
  }

  if (!currentUser || !comment.uid) return;
  if (comment.uid === currentUser.uid) {
    switchMainNav('mylist');
    return;
  }
  if (friends.includes(comment.uid)) {
    await viewUserList(comment.uid, comment.name || 'Anonymous', comment.photo || '');
    return;
  }

  await switchMainNav('community');
  activeFriendsTab = 'find';
  switchFriendsTab('find');
  const searchInput = document.querySelector('#find-people-view .find-search');
  if (searchInput) searchInput.value = comment.name || '';
  searchUsersByUsername(comment.name || '');
  showToast("Find this user to send a friend request");
}


let commentsTransitionOrigin = null;
let commentsTransitionOriginRect = null;
let commentsPageClosing = false;
let commentsCloseAnimation = null;
let commentsRestoreView = null;

function restoreCommentsSourceView() {
  const mainNav = document.querySelector('.main-nav');
  if (mainNav) mainNav.style.display = 'flex';

  const navComm = document.getElementById('nav-community');
  const activeMainTab = navComm && navComm.classList.contains('active') ? 'community' : 'mylist';
  setMainNavVisibility(activeMainTab);

  if (activeMainTab === 'community') {
    loadCommunity();
    return;
  }

  render();
}

function cleanupCommentsPageState() {
  const commentsPageEl = document.getElementById('comments-page');
  commentsPageEl.classList.remove('comments-page-animating', 'comments-page-animating-in', 'comments-page-closing');
  commentsPageEl.style.display = 'none';
  commentsPageEl.style.position = '';
  commentsPageEl.style.left = '';
  commentsPageEl.style.top = '';
  commentsPageEl.style.width = '';
  commentsPageEl.style.height = '';
  commentsPageEl.style.zIndex = '';
  commentsPageEl.style.overflowY = '';
  commentsPageEl.style.opacity = '';
  commentsPageEl.style.transform = '';
  commentsPageEl.style.pointerEvents = '';
  commentsPageEl.style.removeProperty('--comments-origin-x');
  commentsPageEl.style.removeProperty('--comments-origin-y');

  if (typeof commentsRestoreView === 'function') commentsRestoreView();

  commentsItemId = null;
  commentsMediaKey = null;
  commentsScope = 'friends';
  commentsRawItems = [];
  commentsDrafts = { friends: '', global: '' };
  commentsTransitionOrigin = null;
  commentsTransitionOriginRect = null;
  commentsPageClosing = false;
  commentsCloseAnimation = null;
  commentsRestoreView = null;
}

function cancelCommentsCloseIfNeeded() {
  if (!commentsPageClosing) return;
  if (commentsCloseAnimation) {
    commentsCloseAnimation.onfinish = null;
    commentsCloseAnimation.oncancel = null;
    commentsCloseAnimation.cancel();
  }
  cleanupCommentsPageState();
}

function getCommentsTransitionOverlay() {
  let overlay = document.getElementById('comments-transition-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'comments-transition-overlay';
    overlay.className = 'comments-transition-overlay';
    document.body.appendChild(overlay);
  }
  return overlay;
}

function animateCommentsOverlay(fromRect, toRect, done, duration = 420, runDoneOnStart = true) {
  const prefersReduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (prefersReduced || !fromRect || !toRect) {
    done();
    return;
  }

  const overlay = getCommentsTransitionOverlay();
  const startWidth = Math.max(fromRect.width, 44);
  const startHeight = Math.max(fromRect.height, 32);
  const endWidth = Math.max(toRect.width, 44);
  const endHeight = Math.max(toRect.height, 32);
  const dx = fromRect.left - toRect.left;
  const dy = fromRect.top - toRect.top;
  const sx = startWidth / endWidth;
  const sy = startHeight / endHeight;

  overlay.style.left = toRect.left + 'px';
  overlay.style.top = toRect.top + 'px';
  overlay.style.width = endWidth + 'px';
  overlay.style.height = endHeight + 'px';
  overlay.style.opacity = '1';

  const anim = overlay.animate([
    {
      transform: `translate3d(${dx}px, ${dy}px, 0) scale(${sx}, ${sy})`,
      borderRadius: '18px',
      opacity: 0.98
    },
    {
      transform: 'translate3d(0, 0, 0) scale(1, 1)',
      borderRadius: '14px',
      opacity: 0.88,
      offset: 0.72
    },
    {
      transform: 'translate3d(0, 0, 0) scale(1, 1)',
      borderRadius: '14px',
      opacity: 0
    }
  ], {
    duration,
    easing: 'cubic-bezier(0.22, 1, 0.36, 1)',
    fill: 'forwards'
  });

  if (runDoneOnStart) done();

  anim.onfinish = () => {
    overlay.style.opacity = '0';
    overlay.style.width = '0';
    overlay.style.height = '0';
    overlay.style.transform = 'none';
    if (!runDoneOnStart) done();
  };
}

function openCommentsPage(itemId, triggerEl) {
  cancelCommentsCloseIfNeeded();

  const sourceData = getVisibleListData();
  const items = sourceData[activeSection];
  const item = items.find(i => i.id === itemId);
  if (!item) return;

  commentsTransitionOrigin = triggerEl || null;
  commentsTransitionOriginRect = null;
  commentsItemId = itemId;
  commentsMediaKey = getMediaKey(item);
  commentsViewState = { type: 'item', itemId };

  const commentsPageEl = document.getElementById('comments-page');
  const triggerRect = triggerEl ? triggerEl.getBoundingClientRect() : null;
  const startRect = triggerRect ? {
    left: triggerRect.left,
    top: triggerRect.top,
    width: triggerRect.width,
    height: triggerRect.height
  } : null;
  commentsTransitionOriginRect = startRect;

  document.getElementById('mylist-view').style.display = 'none';
  document.getElementById('community-view').style.display = 'none';
  document.getElementById('mylist-header').style.display = 'none';
  document.querySelector('.main-nav').style.display = 'none';
  commentsPageEl.style.display = 'block';
  commentsPageEl.style.pointerEvents = '';
  commentsPageEl.classList.remove('comments-page-animating', 'comments-page-animating-in', 'comments-page-closing');
  commentsPageEl.style.opacity = '1';
  commentsPageEl.style.transform = 'none';

  const emoji = getSectionIcon(activeSection);
  const coverHtml = item.cover
    ? `<div class="comments-page-cover" style="background-image:url('${item.cover}')"></div>`
    : `<div class="comments-page-cover no-img" style="display:flex;align-items:center;justify-content:center;font-size:24px;">${emoji}</div>`;
  const sectionLabel = activeSection === 'anime' ? 'ANIME' : activeSection === 'shows' ? 'TV SHOW' : activeSection === 'movies' ? 'MOVIE' : 'GAME';

  function renderCommentsHeader(yearVal) {
    document.getElementById('comments-page-header').innerHTML = `
      <div class="comments-page-header">
        ${coverHtml}
        <div class="comments-page-info">
          <div class="comments-page-title">${escHtml(item.title)}</div>
          <div class="comments-page-meta">${sectionLabel}${item.genre ? ' · ' + escHtml(item.genre) : ''}${yearVal ? ' · ' + yearVal : ''}</div>
        </div>
      </div>`;
  }

  renderCommentsHeader(item.year);

  if (!item.year && !viewingUser) {
    const tmdbType = isShowSection(activeSection) ? 'tv' : 'movie';
    fetch(buildProxyUrl(TMDB_PROXY_BASE, `search/${tmdbType}`, { query: item.title }))
      .then(r => r.json())
      .then(json => {
        const match = (json.results || [])[0];
        if (match) {
          const yr = (match.release_date || match.first_air_date || '').slice(0, 4);
          if (yr) {
            item.year = yr;
            save();
            renderCommentsHeader(yr);
          }
        }
      }).catch(() => {});
  }

  commentsScope = currentUser ? 'friends' : 'global';
  commentsDrafts = { friends: '', global: '' };
  renderCommentsToolbar();
  renderCommentsInput();
  loadComments();
  persistUiState();
}

function openCommentsPageForActivity(mediaKey, title, cover, commentId = '') {
  cancelCommentsCloseIfNeeded();
  const activityPage = document.getElementById('activity-page');
  const discoverView = document.getElementById('discover-view');
  const commentsPageEl = document.getElementById('comments-page');
  const mainNav = document.querySelector('.main-nav');
  commentsItemId = null;
  commentsMediaKey = mediaKey;
  commentsViewState = { type: 'activity', mediaKey, title, cover, commentId };
  commentsTransitionOrigin = null;
  commentsTransitionOriginRect = null;
  if (activityPage?.classList.contains('active')) {
    activityPage.classList.remove('active');
    commentsRestoreView = () => {
      activityPage.classList.add('active');
      if (commentsPageEl) commentsPageEl.style.display = 'none';
    };
  } else {
    if (discoverView) discoverView.style.display = 'none';
    if (mainNav) mainNav.style.display = 'none';
    commentsRestoreView = () => {
      if (discoverView) discoverView.style.display = 'block';
      if (mainNav) mainNav.style.display = 'flex';
    };
  }
  commentsPageEl.style.display = 'block';
  commentsPageEl.style.pointerEvents = '';
  commentsPageEl.classList.remove('comments-page-animating', 'comments-page-animating-in', 'comments-page-closing');
  commentsPageEl.style.opacity = '1';
  commentsPageEl.style.transform = 'none';
  const coverHtml = cover
    ? `<div class="comments-page-cover" style="background-image:url('${escAttr(cover)}')"></div>`
    : `<div class="comments-page-cover no-img" style="display:flex;align-items:center;justify-content:center;font-size:24px;">💬</div>`;
  document.getElementById('comments-page-header').innerHTML = `
    <div class="comments-page-header">
      ${coverHtml}
      <div class="comments-page-info">
        <div class="comments-page-title">${escHtml(title || 'Comments')}</div>
        <div class="comments-page-meta">FRIEND ACTIVITY</div>
      </div>
    </div>`;
  commentsScope = 'global';
  commentsDrafts = { friends: '', global: '' };
  renderCommentsToolbar();
  renderCommentsInput();
  loadComments();
  persistUiState();
  if (commentId) {
    setTimeout(() => {
      const row = document.querySelector(`#comments-list .comment-item[data-comment-id="${CSS.escape(commentId)}"]`);
      if (row) {
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
        row.animate([
          { backgroundColor: 'rgba(245, 158, 11, 0.18)' },
          { backgroundColor: 'rgba(245, 158, 11, 0)' }
        ], { duration: 1400, easing: 'ease-out' });
      }
    }, 450);
  }
}

function closeCommentsPage() {
  if (commentsPageClosing) return;
  commentsPageClosing = true;

  if (commentsUnsubscribe) { commentsUnsubscribe(); commentsUnsubscribe = null; }

  const commentsPageEl = document.getElementById('comments-page');
  commentsPageEl.classList.remove('comments-page-animating', 'comments-page-animating-in', 'comments-page-closing');
  commentsPageEl.style.pointerEvents = 'none';

  const overlay = document.getElementById('comments-transition-overlay');
  if (overlay) {
    overlay.getAnimations().forEach(anim => anim.cancel());
    overlay.style.opacity = '0';
    overlay.style.width = '0';
    overlay.style.height = '0';
    overlay.style.transform = 'none';
  }

  commentsRestoreView = restoreCommentsSourceView;
  cleanupCommentsPageState();
  commentsViewState = null;
  persistUiState();
}

function loadComments() {
  if (commentsUnsubscribe) commentsUnsubscribe();
  if (isPreviewMode()) {
    commentsUnsubscribe = null;
    commentsRawItems = getPreviewCommentsForMedia(commentsMediaKey);
    commentsRawItems.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    setCachedCommentCount(commentsMediaKey, commentsRawItems.length);
    updateCommentCountBadges(commentsMediaKey, commentsRawItems.length);
    renderCommentsToolbar();
    renderCommentsUI(commentsRawItems);
    return;
  }
  const ref = db.collection('comments').doc(commentsMediaKey);
  commentsUnsubscribe = ref.onSnapshot(doc => {
    commentsRawItems = (doc.exists && doc.data().comments) || [];
    commentsRawItems.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    setCachedCommentCount(commentsMediaKey, commentsRawItems.length);
    updateCommentCountBadges(commentsMediaKey, commentsRawItems.length);
    renderCommentsToolbar();
    renderCommentsUI(commentsRawItems);
  }, err => {
    console.error('Comments listen error:', err);
    commentsRawItems = [];
    setCachedCommentCount(commentsMediaKey, 0);
    updateCommentCountBadges(commentsMediaKey, 0);
    renderCommentsToolbar();
    document.getElementById('comments-list').innerHTML = '<div class="comments-empty">Failed to load comments. Try again in a moment.</div>';
  });
}

function renderCommentsUI(comments) {
  const list = document.getElementById('comments-list');
  const filtered = getScopedComments(comments, commentsScope);
  if (filtered.length === 0) {
    list.innerHTML = `<div class="comments-empty">${getCommentsEmptyMessage(commentsScope)}</div>`;
    return;
  }
  list.innerHTML = filtered.map(c => {
      const photo = c.photo || 'https://ui-avatars.com/api/?name=' + encodeURIComponent(c.name || 'U') + '&background=1e2028&color=60a5fa';
      const isOwn = currentUser && c.uid === currentUser.uid;
      const authorUser = usersMap[c.uid] ? { ...usersMap[c.uid], ...c } : c;
      const authorHtml = commentsScope === 'global' && (c.uid || getPreviewCommunityUser(c.uid))
        ? `<button type="button" class="comment-author-btn" onclick="openCommentAuthorProfile('${c.id}')">${renderDisplayNameHTML(authorUser, 'Anonymous')}</button>`
        : `<span class="comment-author">${renderDisplayNameHTML(authorUser, 'Anonymous')}</span>`;
      return `<div class="comment-item" data-comment-id="${escAttr(c.id || '')}">
      <img class="comment-avatar" src="${photo}" alt="">
      <div class="comment-body">
        <div class="comment-header">
          ${authorHtml}
          <span class="comment-time">${timeAgo(c.timestamp)}</span>
          ${isOwn ? `<button class="comment-delete" onclick="deleteComment('${c.id}')">Delete</button>` : ''}
        </div>
        <div class="comment-text">${escHtml(c.text)}</div>
      </div>
    </div>`;
  }).join('');
}

async function postComment() {
  const input = document.getElementById('comment-textarea');
  const button = document.querySelector('#comments-input-area .comment-post-btn');
  if (!input) return;
  const text = (input.value || '').trim();
  if (!text || !currentUser || !commentsMediaKey || commentsSubmitting) return;

  commentsSubmitting = true;
  if (button) button.disabled = true;
  input.disabled = true;

    const comment = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 7),
      uid: currentUser.uid,
      name: (userProfile && userProfile.name) || currentUser.displayName || 'Anonymous',
      photo: (userProfile && userProfile.photo) || currentUser.photoURL || '',
      accountEmailLower: normalizeEmail(currentUser?.email),
      isCreatorAdmin: normalizeEmail(currentUser?.email) === CREATOR_ADMIN_EMAIL,
      text: text,
    timestamp: Date.now(),
    scope: commentsScope === 'global' ? 'global' : 'friends'
  };

  try {
    const ref = db.collection('comments').doc(commentsMediaKey);
    await db.runTransaction(async transaction => {
      const doc = await transaction.get(ref);
      const existing = (doc.exists && Array.isArray(doc.data().comments)) ? doc.data().comments : [];
      transaction.set(ref, {
        comments: existing.concat(comment)
      }, { merge: true });
      setCachedCommentCount(commentsMediaKey, existing.length + 1);
    });
    commentsDrafts[commentsScope] = '';
    if (document.getElementById('comment-textarea') === input) input.value = '';
    renderCommentsInput();
    updateCommentCountBadges(commentsMediaKey, getCachedCommentCount(commentsMediaKey));
    showToast("Comment posted");
  } catch(e) {
    console.error('Post comment failed:', e);
    showToast("Could not post comment. Try again.");
  } finally {
    commentsSubmitting = false;
    const liveInput = document.getElementById('comment-textarea');
    const liveButton = document.querySelector('#comments-input-area .comment-post-btn');
    if (liveInput) liveInput.disabled = false;
    if (liveButton) liveButton.disabled = false;
  }
}

async function deleteComment(commentId) {
  if (!commentsMediaKey || !currentUser) return;
  try {
    const ref = db.collection('comments').doc(commentsMediaKey);
    await db.runTransaction(async transaction => {
      const doc = await transaction.get(ref);
      if (!doc.exists) return;
      const comments = Array.isArray(doc.data().comments) ? doc.data().comments : [];
      const target = comments.find(c => c.id === commentId);
      if (!target || target.uid !== currentUser.uid) return;
      transaction.set(ref, {
        comments: comments.filter(c => c.id !== commentId)
      }, { merge: true });
      setCachedCommentCount(commentsMediaKey, comments.length - 1);
    });
    updateCommentCountBadges(commentsMediaKey, getCachedCommentCount(commentsMediaKey));
    showToast("Comment deleted");
  } catch(e) {
    console.error('Delete comment failed:', e);
    showToast("Could not delete comment. Try again.");
  }
}

function timeAgo(ts) {
  if (!ts) return '';
  const diff = Date.now() - ts;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return mins + 'm ago';
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return hrs + 'h ago';
  const days = Math.floor(hrs / 24);
  if (days < 30) return days + 'd ago';
  const months = Math.floor(days / 30);
  if (months < 12) return months + 'mo ago';
  return Math.floor(months / 12) + 'y ago';
}

// Auth functions
function signIn() {
  const provider = new firebase.auth.GoogleAuthProvider();
  auth.signInWithPopup(provider).catch(err => {
    console.error("Sign in failed:", err);
    // Fallback for mobile browsers that block popups
    auth.signInWithRedirect(provider);
  });
}

function signOut() {
  stopFriendsDataListener();
  resetFriendsDataState();
  auth.signOut();
}

// Auth state listener
auth.onAuthStateChanged(async (user) => {
  if (user) {
    currentUser = user;
    DOC_REF = db.collection("watchlist").doc(user.uid);
    exitPreviewMode();
    document.getElementById("login-screen").style.display = "none";
    document.getElementById("app-container").style.display = "block";
    await load();
    await saveUserProfile(user);
    bootstrapUserCountIfNeeded();
    startFriendsDataListener(); // live Friends/Requests badge + request list updates
    setDefaultMyListsWatchingView();
    render();
  } else {
    stopFriendsDataListener();
    resetFriendsDataState();
    currentUser = null;
    DOC_REF = null;
    ownDataCache = null;
    myData = null;
    viewingUser = null;
    friendViewData = null;
    syncSignedOutRoute();
  }
});

window.addEventListener('hashchange', syncSignedOutRoute);
window.addEventListener('beforeunload', persistUiState);
