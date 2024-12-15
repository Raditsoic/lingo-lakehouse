import { Disclosure } from '@headlessui/react'

export default function Navbar() {
  return (
    <Disclosure as="nav" className="bg-swan">
      <div className="mx-auto max-w-7xl px-4 sm:px-8 lg:px-12">
        <div className="relative flex h-20 items-center"> 
          <div className="flex flex-1">
            <div className="flex">
              <img
                alt="Duolingo"
                src="./src/assets/landscape-lockup.svg"
                className="h-10" 
              />
            </div>
            <div className="hidden sm:ml-6 sm:block">
              {/* Bisa diisi */}
            </div>
          </div>
          
        </div>
      </div>
    </Disclosure>
  )
}
