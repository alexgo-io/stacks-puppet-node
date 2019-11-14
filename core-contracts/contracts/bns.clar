;;;; Errors
;; todo(ludo): should err be returning strings instead?
(define-constant err-panic 0)
(define-constant err-namespace-preorder-not-found 1001)
(define-constant err-namespace-preorder-expired 1002)
(define-constant err-namespace-preorder-already-exists 1003)
(define-constant err-namespace-unavailable 1004)
(define-constant err-namespace-not-found 1005)
(define-constant err-namespace-already-exists 1006)
(define-constant err-namespace-not-launched 1007)
(define-constant err-namespace-price-function-invalid 1008)
(define-constant err-namespace-preorder-claimability-expired 1009)
(define-constant err-namespace-launchability-expired 1010)
(define-constant err-namespace-operation-unauthorized 1011)
(define-constant err-namespace-stx-burnt-insufficient 1012)
(define-constant err-namespace-blank 1013)
(define-constant err-namespace-already-launched 1014)
(define-constant err-namespace-hash-malformed 1015)
(define-constant err-namespace-charset-invalid 1016)

(define-constant err-name-preorder-not-found 2001)
(define-constant err-name-preorder-expired 2002)
(define-constant err-name-preorder-funds-insufficient 2003)
(define-constant err-name-unavailable 2004)
(define-constant err-name-operation-unauthorized 2006)
(define-constant err-name-stx-burnt-insufficient 2007)
(define-constant err-name-expired 2008)
(define-constant err-name-grace-period 2009)
(define-constant err-name-blank 2010)
(define-constant err-name-already-claimed 2011)
(define-constant err-name-claimability-expired 2012)
(define-constant err-name-not-found 2013)
(define-constant err-name-revoked 2014)
(define-constant err-name-transfer-failed 2015)
(define-constant err-name-preorder-already-exists 2016)
(define-constant err-name-hash-malformed 2017)
(define-constant err-name-preordered-before-namespace-launch 2018)
(define-constant err-name-was-not-registered 2019)
(define-constant err-name-could-not-be-minted 2020)
(define-constant err-name-could-not-be-transfered 2021)
(define-constant err-name-charset-invalid 2022)

(define-constant err-principal-already-associated 3001)
(define-constant err-not-implemented 0)
(define-constant err-insufficient-funds 4001)

;;;; Constants
(define-constant burn-address 'S0000000000000000000002AA028H)

;; TTL
;; todo(ludo): add real values
(define-constant namespace-preorder-claimability-ttl u10)
(define-constant namespace-launchability-ttl u10)
(define-constant name-preorder-claimability-ttl u10)
(define-constant name-lease-duration u10)

;; Price tables
;; todo(ludo): should we have some kind of oracle, for stabilizing name's prices?
(define-constant namespace-prices-tiers (list
  u96000 
  u9600 u9600 
  u960 u960 u960 u960 
  u96 u96 u96 u96 u96 u96 u96 u96 u96 u96 u96 u96 u96))

;; todo(ludo): feature request?
;; (define-type price-function-type (tuple (buckets (list 16 uint))
;;                                         (base uint)
;;                                         (coeff uint)
;;                                         (nonalpha-discount uint)
;;                                         (no-voyel-discount uint)))

;; todo(ludo): replace this temporary token by the native STX
(define-fungible-token stx)

;;;; Data
(define-map namespaces
  ((namespace (buff 20)))
  ((name-importer principal)
   (revealed-at uint)
   (launched-at (optional uint))
   (namespace-version uint)
   (renewal-rule uint)
   (price-function (tuple (buckets (list 16 uint)) (base uint) (coeff uint) (nonalpha-discount uint) (no-voyel-discount uint)))))

(define-map namespace-preorders
  ((hashed-namespace (buff 20)) (buyer principal))
  ((created-at uint) (claimed bool) (stx-burned uint)))

;; todo(ludo): fix name and namespace sizes.
;; according https://docs.blockstack.org/core/wire-format.html
;; "namespace" = 20, "name" = 16? what about subdomains/sponsored names?
(define-non-fungible-token names (tuple (name (buff 16)) (namespace (buff 20))))

(define-map owner-name ((owner principal)) ((name (buff 16)) (namespace (buff 20))))

(define-map name-properties
  ((name (buff 16)) (namespace (buff 20)))
  ((registered-at (optional uint))
   (imported-at (optional uint))
   (revoked-at (optional uint))))
;; todo(ludo): bind a hash of the zonefile

(define-map name-preorders
  ((hashed-fqn (buff 20)) (buyer principal))
  ((created-at uint) (claimed bool) (stx-burned uint)))

(define-map zonefiles
  ((name (buff 16)) (namespace (buff 20)))
  ((content (buff 40960)) (updated-at uint)))

(define-map sponsors
  ((name (buff 20)) (namespace (buff 20)))
  ((content (buff 40960))))

(define-private (min (a uint) (b uint))
  (if (<= a b) a b))

(define-private (max (a uint) (b uint))
  (if (> a b) a b))

(define-private (compute-namespace-price (namespace (buff 20)))
  (let ((namespace-len (len namespace)))
    (asserts!
      (> namespace-len u0)
      (err err-namespace-blank))
    (ok (get value (fold 
      element-at 
      namespace-prices-tiers 
      (tuple (limit (min u8 namespace-len)) (cursor u0) (value u0)))))))

(define-private (element-at (i uint) (acc (tuple (limit uint) (cursor uint) (value uint))))
  (if (eq? (get cursor acc) (get limit acc))
    (tuple (limit (get limit acc)) (cursor (+ u1 (get cursor acc))) (value i))
    (tuple (limit (get limit acc)) (cursor (+ u1 (get cursor acc))) (value (get value acc)))))
  
(define-private (get-exp-at-index (buckets (list 16 uint)) (index uint))
  (get value (fold element-at buckets (tuple (limit index) (cursor u0) (value u0)))))

;; todo(ludo): feature request (and (>= char 0x30) (<= char 0x39)) ?
(define-private (is-digit (char (buff 1)))
  (or 
    (eq? char "0") 
    (eq? char "1") 
    (eq? char "2") 
    (eq? char "3") 
    (eq? char "4") 
    (eq? char "5") 
    (eq? char "6") 
    (eq? char "7") 
    (eq? char "8") 
    (eq? char "9")))

(define-private (is-downcased-alpha (char (buff 1)))
  (or 
    (eq? char "a") 
    (eq? char "b") 
    (eq? char "c") 
    (eq? char "d") 
    (eq? char "e") 
    (eq? char "f") 
    (eq? char "g") 
    (eq? char "h") 
    (eq? char "i") 
    (eq? char "j") 
    (eq? char "k") 
    (eq? char "l") 
    (eq? char "m") 
    (eq? char "n") 
    (eq? char "o") 
    (eq? char "p") 
    (eq? char "q") 
    (eq? char "r") 
    (eq? char "s") 
    (eq? char "t") 
    (eq? char "u") 
    (eq? char "v") 
    (eq? char "w") 
    (eq? char "x") 
    (eq? char "y") 
    (eq? char "z")))

(define-private (is-vowel (char (buff 1)))
  (or 
    (eq? char "a") 
    (eq? char "e") 
    (eq? char "i") 
    (eq? char "o") 
    (eq? char "u") 
    (eq? char "y")))

(define-private (is-special-char (char (buff 1)))
  (or 
    (eq? char "-") 
    (eq? char "_")))

(define-private (is-char-valid (char (buff 1)))
  (or 
    (is-downcased-alpha char)
    (is-digit char)
    (is-special-char char)))

(define-private (is-nonalpha (char (buff 1)))
  (or 
    (is-digit char)
    (is-special-char char)))

(define-private (has-vowels-chars (name (buff 16)))
  (> (len (filter is-vowel name)) u0))

(define-private (has-nonalpha-chars (name (buff 16)))
  (> (len (filter is-nonalpha name)) u0))

(define-private (has-invalid-chars (name (buff 20)))
  (< (len (filter is-char-valid name)) (len name)))

(define-private (compute-name-price (name (buff 16))
                                    (price-function (tuple (buckets (list 16 uint)) (base uint) (coeff uint) (nonalpha-discount uint) (no-voyel-discount uint))))
  (let (
    (exponent (get-exp-at-index (get buckets price-function) (min u15 (- (len name) u1))))
    (no-voyel-discount (if (not (has-vowels-chars name)) (get no-voyel-discount price-function) u1))
    (nonalpha-discount (if (has-nonalpha-chars name) (get nonalpha-discount price-function) u1)))
    (*
      (/
        (*
          (get coeff price-function)
          (pow (get base price-function) exponent))
        (max nonalpha-discount no-voyel-discount))
      u10))) ;; 10 = name_cost (100) * "old_price_multiplier" (0.1) - todo(ludo): sort this out.

(define-private (is-name-lease-expired (namespace (buff 20)) (name (buff 16)))
  (let ((name-props (expects! 
    (map-get name-properties ((namespace namespace) (name name))) 
    (err err-name-not-found))))
    (let ((registered-at (expects! 
      (get registered-at name-props) 
      (err err-name-was-not-registered))))
      (ok (> block-height (+ name-lease-duration registered-at))))))

;; todo(ludo): to implement
(define-private (is-name-in-grace-period (namespace (buff 20)) (name (buff 16)))
  'false)

;;;; NAMESPACES

;; NAMESPACE_PREORDER
;; This step registers the salted hash of the namespace with BNS nodes, and burns the requisite amount of cryptocurrency.
;; Additionally, this step proves to the BNS nodes that user has honored the BNS consensus rules by including a recent
;; consensus hash in the transaction.
;; Returns pre-order's expiration date (in blocks).
(define-public (namespace-preorder (hashed-namespace (buff 20))
                                   (stx-to-burn uint))
  (let 
    ((former-preorder 
      (map-get namespace-preorders ((hashed-namespace hashed-namespace) (buyer contract-caller)))))
    ;; Ensure eventual former pre-order expired 
    (asserts! 
      (if (is-none? former-preorder)
        'true
        (>= block-height (+ namespace-preorder-claimability-ttl ;; todo(ludo): settle on [created-at created-at+ttl[ or [created-at created-at+ttl]
                            (expects! (get created-at former-preorder) (err err-panic)))))
      (err err-namespace-preorder-already-exists))
          (asserts! (> stx-to-burn u0) (err err-namespace-stx-burnt-insufficient))
    ;; Ensure that the hashed namespace is 20 bytes long
    (asserts! (eq? (len hashed-namespace) u20) (err err-namespace-hash-malformed))
    ;; Ensure that user will be burning a positive amount of tokens
    (asserts! (> stx-to-burn u0) (err err-namespace-stx-burnt-insufficient))
    ;; Burn the tokens - todo(ludo): switch to native STX once available
    (expects! (ft-transfer! stx stx-to-burn tx-sender burn-address) (err err-insufficient-funds))
    ;; Register the preorder
    (map-set! namespace-preorders
      ((hashed-namespace hashed-namespace) (buyer contract-caller))
      ((created-at block-height) (claimed 'false) (stx-burned stx-to-burn)))
    ;; todo(ludo): don't improvise, look at the returned values in the codebase
    (ok (+ block-height namespace-preorder-claimability-ttl))))

;; NAMESPACE_REVEAL
;; This second step reveals the salt and the namespace ID (pairing it with its NAMESPACE_PREORDER). It reveals how long
;; names last in this namespace before they expire or must be renewed, and it sets a price function for the namespace
;; that determines how cheap or expensive names its will be.
;; Note (1): in the original implementation, the hash of the namespace is being salted so that 2 users preordering the same
;; namespace wouldn't collide. In this implementation, the hashed-namespace is associated in a tuple, with the principal
;; of the user.
(define-public (namespace-reveal (namespace (buff 20))
                                 (namespace-version uint)
                                 (namespace-salt (buff 20))
                                 (p-func-base uint)
                                 (p-func-coeff uint)
                                 (p-func-b1 uint)
                                 (p-func-b2 uint)
                                 (p-func-b3 uint)
                                 (p-func-b4 uint)
                                 (p-func-b5 uint)
                                 (p-func-b6 uint)
                                 (p-func-b7 uint)
                                 (p-func-b8 uint)
                                 (p-func-b9 uint)
                                 (p-func-b10 uint)
                                 (p-func-b11 uint)
                                 (p-func-b12 uint)
                                 (p-func-b13 uint)
                                 (p-func-b14 uint)
                                 (p-func-b15 uint)
                                 (p-func-b16 uint)
                                 (p-func-non-alpha-discount uint)
                                 (p-func-no-voyel-discount uint)
                                 (renewal-rule uint)
                                 (name-importer principal))
  ;; The salt and namespace must hash to a preorder entry in the `namespace_preorders` table.
  ;; The sender must match the principal in the preorder entry (implied)
  (let (
    (hashed-namespace (hash160 (concat namespace namespace-salt)))
    (price-function (tuple 
      (buckets (list
        p-func-b1
        p-func-b2
        p-func-b3
        p-func-b4
        p-func-b5
        p-func-b6
        p-func-b7
        p-func-b8
        p-func-b9
        p-func-b10
        p-func-b11
        p-func-b12
        p-func-b13
        p-func-b14
        p-func-b15
        p-func-b16))
      (base p-func-base)
      (coeff p-func-coeff)
      (nonalpha-discount p-func-non-alpha-discount)
      (no-voyel-discount p-func-no-voyel-discount))))
    (let (
      (preorder (expects!
        (map-get namespace-preorders ((hashed-namespace hashed-namespace) (buyer tx-sender))) ;; todo(ludo): tx-sender or contract-caller?
        (err err-namespace-preorder-not-found)))
      (namespace-price (expects! 
        (compute-namespace-price namespace)
        (err err-namespace-blank))))
    ;; The namespace must only have valid chars
    (asserts!
      (not (has-invalid-chars namespace))
      (err err-namespace-charset-invalid))
    ;; The namespace must not exist yet in the `namespaces` table
    (asserts!
      (is-none? (map-get namespaces ((namespace namespace))))
      (err err-namespace-already-exists))
    ;; The amount burnt must be equal to or greater than the cost of the namespace
    (asserts!
      (>= (get stx-burned preorder) namespace-price)
      (err err-namespace-stx-burnt-insufficient))
    ;; todo(ludo): validate the price function inputs
    ;; This transaction must arrive within 24 hours of its `NAMESPACE_PREORDER`
    (asserts!
      (< block-height (+ (get created-at preorder) namespace-preorder-claimability-ttl))
      (err err-namespace-preorder-claimability-expired))
    ;; The preorder record for this namespace will be marked as "claimed"
    (map-set! namespace-preorders
      ((hashed-namespace hashed-namespace) (buyer tx-sender))
      ((created-at (get created-at preorder)) (claimed 'true) (stx-burned (get stx-burned preorder))))
    ;; The namespace will be set as "revealed" but not "launched", its price function, its renewal rules, its version,
    ;; and its import principal will be written to the  `namespaces` table.
    ;; Name should be lowercased.
    (ok (map-set! namespaces
      ((namespace namespace))
      ((name-importer name-importer)
       (revealed-at block-height)
       (launched-at none)
       (namespace-version namespace-version)
       (renewal-rule renewal-rule)
       (price-function price-function)))))))

;; NAME_IMPORT
;; Once a namespace is revealed, the user has the option to populate it with a set of names. Each imported name is given
;; both an owner and some off-chain state. This step is optional; Namespace creators are not required to import names.
(define-public (name-import (namespace (buff 20))
                            (name (buff 16))
                            (zonefile-content (buff 40960)))
  (let ((namespace-props
        (expects!
          (map-get namespaces ((namespace namespace)))
          (err err-namespace-not-found))))
    ;; The name's namespace must not be launched
    (asserts!
      (is-none? (get launched-at namespace-props))
      (err err-namespace-already-launched))
    ;; Less than 1 year must have passed since the namespace was "revealed"
    (asserts!
      (< block-height (+ (get revealed-at namespace-props) namespace-launchability-ttl))
      (err err-namespace-launchability-expired))
    ;; The sender principal must match the namespace's import principal
    (asserts!
      (eq? (get name-importer namespace-props) tx-sender) ;; todo(ludo): tx-sender or contract-caller?
      (err err-namespace-operation-unauthorized))
    ;; Mint the new name
    (nft-mint! names (tuple (namespace namespace) (name name)) tx-sender) ;; todo(ludo): tx-sender or contract-caller? nft-mint! or nft-mint? ?
    ;; The namespace will be set as "revealed" but not "launched", its price function, its renewal rules, its version, and its import principal will be written to the  `namespaces` table
    (map-set! name-properties
      ((namespace namespace) (name name))
      ((registered-at none)
       (imported-at (some block-height))
       (revoked-at none)))
    ;; Import the zonefile
    (map-set! zonefiles
      ((namespace namespace) (name name))
      ((updated-at block-height)
       (content zonefile-content)))
    (ok 'true)))

;; NAMESPACE_READY
;; The final step of the process launches the namespace and makes the namespace available to the public. Once a namespace
;; is launched, anyone can register a name in it if they pay the appropriate amount of cryptocurrency.
(define-public (namespace-ready (namespace (buff 20)))
  (let ((namespace-props
        (expects!
          (map-get namespaces ((namespace namespace)))
          (err err-namespace-not-found))))
    ;; The name's namespace must not be launched
    (asserts!
      (is-none? (get launched-at namespace-props))
      (err err-namespace-already-launched))
    ;; Less than 1 year must have passed since the namespace was "revealed"
    (asserts!
      (< block-height (+ (get revealed-at namespace-props) namespace-launchability-ttl))
      (err err-namespace-launchability-expired))
    ;; todo(ludo): Check owner-name
    ;; The sender principal must match the namespace's import principal
    (asserts!
      (eq? (get name-importer namespace-props) tx-sender) ;; todo(ludo): tx-sender or contract-caller?
      (err err-namespace-operation-unauthorized))
    ;; The namespace will be set as "revealed" but not "launched", its price function, its renewal rules, its version, and its import principal will be written to the  `namespaces` table
    (ok (map-set! namespaces
      ((namespace namespace))
      ((launched-at (some block-height))
       (name-importer (get name-importer namespace-props))
       (revealed-at (get revealed-at namespace-props))
       (namespace-version (get namespace-version namespace-props))
       (renewal-rule (get renewal-rule namespace-props))
       (price-function (get price-function namespace-props)))))))

;;;; NAMES

;; NAME_PREORDER
;; This is the first transaction to be sent. It tells all BNS nodes the salted hash of the BNS name,
;; and it pays the registration fee to the namespace owner's designated address
(define-public (name-preorder (hashed-fqn (buff 20))
                              (stx-to-burn uint))
  (let 
    ((former-preorder 
      (map-get name-preorders ((hashed-fqn hashed-fqn) (buyer tx-sender)))))
    ;; Ensure eventual former pre-order expired 
    (asserts! 
      (if (is-none? former-preorder)
        'true
        (>= block-height (+ name-preorder-claimability-ttl ;; todo(ludo): settle on [created-at created-at+ttl[ or [created-at created-at+ttl]
                            (expects! (get created-at former-preorder) (err err-panic)))))
      (err err-name-preorder-already-exists))
          (asserts! (> stx-to-burn u0) (err err-namespace-stx-burnt-insufficient))    
    ;; Ensure that the hashed fqn is 20 bytes long
    (asserts! (eq? (len hashed-fqn) u20) (err err-name-hash-malformed))
    ;; Ensure that user will be burning a positive amount of tokens
    (asserts! (> stx-to-burn u0) (err err-name-stx-burnt-insufficient))
    ;; Burn the tokens - todo(ludo): switch to native STX once available
    (expects! (ft-transfer! stx stx-to-burn tx-sender burn-address) (err err-insufficient-funds)) ;; todo(ludo): tx-sender or contract-caller?
    ;; Register the pre-order
    (map-set! name-preorders
      ((hashed-fqn hashed-fqn) (buyer tx-sender))
      ((created-at block-height) (stx-burned stx-to-burn) (claimed 'false)))
    (ok (+ block-height name-preorder-claimability-ttl))))

;; NAME_REGISTRATION
;; This is the second transaction to be sent. It reveals the salt and the name to all BNS nodes,
;; and assigns the name an initial public key hash and zone file hash
;; todo(ludo): should we clean / expire the pre-order if something wrong happened? 
(define-public (name-register (namespace (buff 20))
                              (name (buff 16))
                              (zonefile-content (buff 40960)))
  (let (
    (hashed-fqn (hash160 (concat (concat name ".") namespace)))
    (name-currently-owned (map-get owner-name ((owner tx-sender)))))
    (let ( 
        (preorder (expects!
          (map-get name-preorders ((hashed-fqn hashed-fqn) (buyer tx-sender))) ;; todo(ludo): tx-sender or contract-caller?
          (err err-name-preorder-not-found)))
        (namespace-props (expects!
          (map-get namespaces ((namespace namespace)))
          (err err-namespace-not-found)))
        (current-owner (nft-get-owner names (tuple (name name) (namespace namespace)))) ;; todo(ludo): implicit tuple syntax not available here.
        (can-sender-register-name (if (is-none? name-currently-owned)
                                 'true
                                  (expects! 
                                    (is-name-lease-expired
                                      (expects! (get namespace name-currently-owned) (err err-panic))
                                      (expects! (get name name-currently-owned) (err err-panic)))
                                    (err err-panic)))))
      ;; The name must only have valid chars
      (asserts!
        (not (has-invalid-chars name))
        (err err-name-charset-invalid))
      ;; The name must not exist yet, or be expired - todo(ludo): existing name transfers not handled
      (if (is-none? current-owner)
        'true
        (asserts!
          (expects! (is-name-lease-expired namespace name) (err err-panic))
          (err err-name-unavailable)))
      ;; The name's namespace must be launched
      (asserts!
        (eq? (is-none? (get launched-at namespace-props)) 'false) ;; todo(ludo): is-some? would be more readable.
        (err err-namespace-not-launched))
      ;; The preorder must have been created after the launch of the namespace
      (asserts!
        (> (get created-at preorder) (expects! (get launched-at namespace-props) (err err-panic)))
        (err err-name-preordered-before-namespace-launch))
      ;; The preorder entry must be unclaimed - todo(ludo): is this assertion redundant?
      (asserts!
        (eq? (get claimed preorder) 'false)
        (err err-name-already-claimed))
      ;; Less than 24 hours must have passed since the name was preordered
      (asserts!
        (< block-height (+ (get created-at preorder) name-preorder-claimability-ttl))
        (err err-name-claimability-expired))
      ;; The amount burnt must be equal to or greater than the cost of the name
      (asserts!
        (>= (get stx-burned preorder) (compute-name-price name (get price-function namespace-props)))
        (err err-name-stx-burnt-insufficient))
      ;; The principal can register a name
      (asserts!
        can-sender-register-name
        (err err-principal-already-associated))
      ;; Mint / Transfer the name
      (if (is-none? current-owner)
        (expects! 
          (nft-mint! 
            names 
            (tuple (namespace namespace) (name name)) 
            tx-sender)  ;; todo(ludo): tx-sender or contract-caller?
          (err err-name-could-not-be-minted))
        (expects!
          (nft-transfer!
            names
            (tuple (name name) (namespace namespace))
            (expects! current-owner (err err-panic))
            tx-sender) ;; tx-sender or contract-caller?
          (err err-name-could-not-be-transfered)))
      ;; Update name's metadata / properties
      (map-set! name-properties
        ((namespace namespace) (name name))
        ((registered-at (some block-height))
        (imported-at none)
        (revoked-at none)))
      (map-set! owner-name
        ((owner tx-sender))
        ((namespace namespace) (name name)))
      ;; Import the zonefile - todo(ludo): keep a hash of the zonefile in name-properties
      (map-set! zonefiles
        ((namespace namespace) (name name))
        ((updated-at block-height)
        (content zonefile-content)))
      (ok 'true))))

;; NAME_UPDATE
(define-public (name-update (namespace (buff 20))
                            (name (buff 16))
                            (zonefile-content (buff 40960)))
  (let (
    (owner (expects!
      (nft-get-owner names (tuple (name name) (namespace namespace)))
      (err err-name-not-found))) ;; The name must exist
    (name-props (expects!
      (map-get name-properties ((name name) (namespace namespace)))
      (err err-name-not-found)))) ;; The name must exist
    ;; The sender must match the name's current owner
    (asserts!
      (eq? owner tx-sender) ;; todo(ludo): tx-sender or contract-caller?
      (err err-name-operation-unauthorized))
    ;; The name must not be expired
    (asserts!
      (eq? (expects! (is-name-lease-expired namespace name) (err err-panic)) 'false)
      (err err-name-expired))
    ;; The name must not be in the renewal grace period
    (asserts!
      (eq? (is-name-in-grace-period namespace name) 'false)
      (err err-name-grace-period))
    ;; The name must not be revoked
    (asserts!
      (is-none? (get revoked-at name-props))
      (err err-name-revoked))
    ;; Update the zonefile
    (map-set! zonefiles
      ((namespace namespace) (name name))
      ((updated-at block-height)
       (content zonefile-content)))
    (ok 'true)))

;; NAME_TRANSFER
(define-public (name-transfer (namespace (buff 20))
                              (name (buff 16))
                              (new-owner principal)
                              (zonefile-content (optional (buff 40960))))
  (let ((current-owned-name (map-get owner-name ((owner new-owner)))))
    (let (
      (owner (expects!
        (nft-get-owner names (tuple (name name) (namespace namespace)))
        (err err-name-not-found))) ;; The name must exist
      (name-props (expects!
        (map-get name-properties ((name name) (namespace namespace)))
        (err err-name-not-found))) ;; The name must exist
      (can-new-owner-get-name (if (is-none? current-owned-name)
                                  'true
                                  (expects! 
                                    (is-name-lease-expired
                                      (expects! (get namespace current-owned-name) (err err-panic))
                                      (expects! (get name current-owned-name) (err err-panic)))
                                    (err err-panic)))))
      ;; The sender must match the name's current owner
      (asserts!
        (eq? owner tx-sender) ;; todo(ludo): tx-sender or contract-caller?
        (err err-name-operation-unauthorized))
      ;; The name must not be expired
      (asserts! 'false (err err-panic))
      (asserts!
        (eq? (expects! (is-name-lease-expired namespace name) (err err-panic)) 'false)
        (err err-name-expired))
      ;; The name must not be in the renewal grace period
      (asserts!
        (eq? (is-name-in-grace-period namespace name) 'false)
        (err err-name-grace-period))
      ;; The name must not be revoked
      (asserts!
        (is-none? (get revoked-at name-props))
        (err err-name-revoked))
      ;; The new owner does not own a name
      (asserts!
        can-new-owner-get-name
        (err err-principal-already-associated))
      ;; Transfer the name
      (expects!
        (nft-transfer! names
                      (tuple (name name) (namespace namespace))
                      tx-sender
                      new-owner) ;; tx-sender or contract-caller?
        (err err-name-transfer-failed))
      (map-set! owner-name
        ((owner new-owner))
        ((namespace namespace) (name name)))
      ;; Update the zonefile, if any.
      (if (is-none? zonefile-content)
        'true
        (map-set! zonefiles
          ((namespace namespace) (name name))
          ((updated-at block-height)
          (content (expects! zonefile-content (err err-panic))))))
      (ok 'true))))

;; NAME_REVOKE
(define-public (name-revoke (namespace (buff 20))
                            (name (buff 16)))
  (let (
    (owner (expects!
      (nft-get-owner names (tuple (name name) (namespace namespace)))
      (err err-name-not-found))) ;; The name must exist
    (name-props (expects!
      (map-get name-properties ((name name) (namespace namespace)))
      (err err-name-not-found)))) ;; The name must exist
    ;; The sender must match the name's current owner
    (asserts!
      (eq? owner tx-sender) ;; todo(ludo): tx-sender or contract-caller?
      (err err-name-operation-unauthorized))
    ;; The name must not be expired
    (asserts!
      (eq? (has-name-expired namespace name) 'false) ;; todo(ludo): refactor has-name-expired signature?
      (err err-name-expired))
    ;; The name must not be in the renewal grace period
    (asserts!
      (eq? (is-name-in-grace-period namespace name) 'false) ;; todo(ludo): refactor is-name-in-grace-period signature?
      (err err-name-grace-period))
    ;; The name must not be revoked
    (asserts!
      (is-none? (get revoked-at name-props))
      (err err-name-revoked))
    ;; Delete the zonefile
    (map-set! name-properties
      ((namespace namespace) (name name))
      ((registered-at (get registered-at name-props))
       (imported-at (get imported-at name-props))
       (revoked-at (some block-height))))
    (ok 'true)))

;; NAME_RENEWAL
(define-public (name-renewal (namespace (buff 20))
                             (name (buff 16))
                             (stx-to-burn uint)
                             (new-owner (optional principal))
                             (zonefile-content (optional (buff 40960))))
  (let (
    (namespace-props (expects!
      (map-get namespaces ((namespace namespace)))
      (err err-namespace-not-found)))
    (owner (expects!
      (nft-get-owner names (tuple (name name) (namespace namespace)))
      (err err-name-not-found))) ;; The name must exist
    (name-props (expects!
      (map-get name-properties ((name name) (namespace namespace)))
      (err err-name-not-found)))) ;; The name must exist
    ;; The sender must match the name's current owner
    (asserts!
      (eq? owner tx-sender) ;; todo(ludo): tx-sender or contract-caller?
      (err err-name-operation-unauthorized))
    ;; The name must not be expired
    (asserts!
      (eq? (has-name-expired namespace name) 'false) ;; todo(ludo): refactor has-name-expired signature?
      (err err-name-expired))
    ;; The name must not be in the renewal grace period
    (asserts!
      (eq? (is-name-in-grace-period namespace name) 'false) ;; todo(ludo): refactor is-name-in-grace-period signature?
      (err err-name-grace-period))
    ;; The amount burnt must be equal to or greater than the cost of the namespace
    (asserts!
      (> stx-to-burn (compute-name-price name (get price-function namespace-props)))
      (err err-name-stx-burnt-insufficient))
    ;; The name must not be revoked
    (asserts!
      (is-none? (get revoked-at name-props))
      (err err-name-revoked))
    ;; Transfer the name, if any new-owner
    (if (is-none? new-owner)
      (ok 'false) 
      (let ((owner-unwrapped (expects! new-owner (err err-panic))))
        (let ((current-owned-name (map-get owner-name ((owner owner-unwrapped)))))
          (let ((can-new-owner-get-name (if (is-none? current-owned-name)
                                  'true
                                  (has-name-expired 
                                   (expects! (get namespace current-owned-name) (err err-panic))
                                   (expects! (get name current-owned-name) (err err-panic))))))
            (asserts!
              can-new-owner-get-name
              (err err-principal-already-associated))
            (expects!
              (nft-transfer! names
                            (tuple (name name) (namespace namespace))
                            tx-sender
                            owner-unwrapped) ;; todo(ludo): tx-sender or contract-caller?. Unwrap new-owner
              (err err-name-transfer-failed))
            (ok 'true)))))
        ;; Update the zonefile, if any.
    (if (is-none? zonefile-content)
      'false
      (map-set! zonefiles
        ((namespace namespace) (name name))
        ((updated-at block-height)
         (content (expects! zonefile-content (err err-panic))))))
    ;; Update the name's properties
    (map-set! name-properties
      ((namespace namespace) (name name))
      ((registered-at (some block-height))
       (imported-at none)
       (revoked-at none)))
    (ok 'true)))

;; Additionals public methods

(define-public (can-name-be-registered (namespace (buff 20)) (name (buff 16)))
  (let (
      (wrapped-name-props (map-get name-properties ((namespace namespace) (name name))))
      (current-owner (map-get owner-name ((owner tx-sender))))
      (namespace-props (expects! (map-get namespaces ((namespace namespace))) (ok 'false))))
    ;; Ensure that namespace has been launched 
    (expects! (get launched-at namespace-props) (ok 'false))
    ;; Early return - Name has never be minted
    (asserts! (is-none? (nft-get-owner names (tuple (name name) (namespace namespace)))) (ok 'true))
    ;; Integrity check - Ensure that we have some entries in nft && owner-name && name-props
    (asserts! 
      (and 
        (not (is-none? wrapped-name-props))
        (not (is-none? current-owner)))
      (err err-panic))
    (let ((name-props (expects! wrapped-name-props (err err-panic))))
      ;; Early return - Name has been revoked and can be registered
      (asserts! (not (is-none? (get revoked-at name-props))) (ok 'true))
      ;; Integrity check - Ensure that the name was either "imported" or "registered".
      (asserts! 
        (or 
          (and (not (is-none? (get registered-at name-props))) (is-none? (get imported-at name-props)))
          (and (not (is-none? (get imported-at name-props))) (is-none? (get registered-at name-props))))
        (err err-panic))
      ;; Ensure that name was not imported (vs registered) - no expiration
      (asserts! (is-none? (get registered-at name-props)) (ok 'false))
      ;; Is lease expired?
      (ok (> block-height (+ name-lease-duration (expects! (get registered-at name-props) (err err-panic))))))))

(define-public (get-name-zonefile (namespace (buff 20)) (name (buff 16)))
  (let (
    (owner (expects!
      (nft-get-owner names (tuple (name name) (namespace namespace)))
      (err err-name-not-found))) ;; The name must exist
    (name-props (expects!
      (map-get name-properties ((name name) (namespace namespace)))
      (err err-name-not-found)))
    (is-lease-expired (is-name-lease-expired namespace name)))
    ;; The name must not be expired
    (if (is-ok? is-lease-expired)
      (asserts! (not (expects! is-lease-expired (err err-panic))) (err err-name-expired))
      'true)
    ;; The name must not be in the renewal grace period
    (asserts!
      (eq? (is-name-in-grace-period namespace name) 'false) ;; todo(ludo): refactor is-name-in-grace-period signature?
      (err err-name-grace-period))
    ;; The name must not be revoked
    (asserts!
      (is-none? (get revoked-at name-props))
      (err err-name-revoked))
    ;; Get the zonefile
    (ok (expects! 
      (get content (map-get zonefiles ((namespace namespace) (name name))))
      (err 9)))))

;; todo(ludo): to be removed
(begin
  (ft-mint! stx u10000000000 'SP2J6ZY48GV1EZ5V2V5RB9MP66SW86PYKKNRV9EJ7)
  (ft-mint! stx u10000000 'S02J6ZY48GV1EZ5V2V5RB9MP66SW86PYKKPVKG2CE)
  (ft-mint! stx u10000000 'SZ2J6ZY48GV1EZ5V2V5RB9MP66SW86PYKKQ9H6DPR))

